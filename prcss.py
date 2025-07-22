from flask import Flask, request
import datetime
import json
import psycopg2
import re
from cryptography.fernet import Fernet
import os
from datetime import datetime, timedelta, timezone
import pytz

app = Flask(__name__)

def cargar_config_db():
    try:
        with open("key.key", "rb") as key_file:
            key = key_file.read()

        fernet = Fernet(key)

        with open("db.txt", "rb") as enc_file:
            encrypted_data = enc_file.read()

        decrypted_data = fernet.decrypt(encrypted_data).decode()

        # Evalúa el string como diccionario de Python
        namespace = {}
        exec(decrypted_data, {}, namespace)
        return namespace["DB_CONFIG"]

    except Exception as e:
        print("❌ Error cargando configuración de la base de datos:", e)
        return None

# Llama a la función para cargar DB_CONFIG
DB_CONFIG = cargar_config_db()

#
def conectar_db():
    return psycopg2.connect(**DB_CONFIG)

def asignar_evento_ajustado_por_email(email):
    try:
        conn = conectar_db()
        cur = conn.cursor()

        cur.execute("""
            UPDATE mails_open
            SET evento_ajustado = CASE
                WHEN event_type = 'email_sent' THEN 'Sent'
                WHEN event_type = 'email_bounced' THEN 'Bounce'
                WHEN event_type = 'Replied' THEN 'Replied'
                WHEN event_type = 'Auto Reply' THEN 'Auto Reply'
                WHEN event_type = 'Interested' THEN 'Interested'
                WHEN event_type = 'Unibox Reply' THEN 'Unibox Reply'
                WHEN event_type = 'Meeting booked' THEN 'Meeting booked'
                WHEN event_type = 'Customer' THEN 'Customer'
                WHEN event_type = 'email_opened' THEN 'Opened'
                WHEN event_type = 'link_clicked' THEN 'Link Clicked'
                ELSE evento_ajustado
            END
            WHERE email = %s;
        """, (email,))

        cur.execute("""
            UPDATE mails_open mo
            SET evento_ajustado = 'Opened False'
            FROM (
                SELECT o1.id
                FROM mails_open o1
                JOIN mails_open o2 
                  ON o1.email = o2.email
                 AND o1.step = o2.step
                 AND o2.event_type = 'email_sent'
                WHERE o1.event_type = 'email_opened'
                  AND o1.email = %s
                  AND EXTRACT(EPOCH FROM (o1.created_at - o2.created_at)) < 60
            ) sub
            WHERE mo.id = sub.id;

            UPDATE mails_open
            SET evento_ajustado = 'Opened Bounce'
            WHERE event_type = 'email_opened' AND email = %s
            AND EXISTS (
                SELECT 1 FROM mails_open m2
                WHERE m2.email = mails_open.email
                  AND m2.step = mails_open.step
                  AND m2.event_type = 'email_bounced'
            );

            UPDATE mails_open
            SET evento_ajustado = 'Link Clicked False'
            WHERE event_type = 'link_clicked' AND email = %s
            AND (
                EXISTS (
                    SELECT 1 FROM mails_open m2
                    WHERE m2.email = mails_open.email
                      AND m2.step = mails_open.step
                      AND m2.evento_ajustado = 'Opened False'
                      AND m2.created_at < mails_open.created_at
                )
                OR NOT EXISTS (
                    SELECT 1 FROM mails_open m3
                    WHERE m3.email = mails_open.email
                      AND m3.step = mails_open.step
                      AND m3.evento_ajustado = 'Opened'
                      AND m3.created_at < mails_open.created_at
                )
            );

            UPDATE mails_open
            SET evento_ajustado = 'Link Clicked Bounce'
            WHERE event_type = 'link_clicked' AND email = %s
            AND EXISTS (
                SELECT 1 FROM mails_open m2
                WHERE m2.email = mails_open.email
                  AND m2.step = mails_open.step
                  AND m2.event_type = 'email_bounced'
            );
        """, (email, email, email, email))

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ evento_ajustado calculado automáticamente para {email}.")

    except Exception as e:
        print("❌ Error actualizando evento_ajustado:", e)

def insertar_evento(data):
    try:
        conn = conectar_db()
        cur = conn.cursor()

        email = data.get("lead_email") or data.get("email")
        if not email:
            print("❌ No se puede registrar evento sin email.")
            return

        first_name = data.get("firstName", "")
        last_name = data.get("lastName", "")
        title = data.get("jobTitle", "")
        linkedin = data.get("linkedIn", "")

        # 🚀 Extraer cliente y campaña del campaign_name más robusto
        raw_campaign = data.get("campaign_name", "")
        cliente_match = re.search(r"\((.*?)\)", raw_campaign)
        cliente = cliente_match.group(1).strip() if cliente_match else ""
        campaign_clean = re.sub(r"\(.*?\)", "", raw_campaign).strip()

        # DEBUG print
        print(f"🚀 Procesado campaign_name='{raw_campaign}' => cliente='{cliente}', campaña='{campaign_clean}'")

        # Insertar contacto si no existe
        cur.execute("""
            INSERT INTO contactos (email, first_name, last_name, title, linkedin)
            SELECT %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM contactos WHERE email = %s
            )
            RETURNING id;
        """, (email, first_name, last_name, title, linkedin, email))

        inserted = cur.fetchone()
        if inserted:
            contacto_id = inserted[0]
        else:
            cur.execute("SELECT id FROM contactos WHERE email = %s", (email,))
            contacto_id = cur.fetchone()[0]

        # Insertar en mails_open
        cur.execute("""
            INSERT INTO mails_open (
                contacto_id, id_user, timestamp, event_type, workspace,
                campaign_id, unibox_url, campaign_name, email_account,
                lead_email, phone, job_title, company_name,
                linkedin, step, variant, email, cliente
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            contacto_id, None,
            data.get("timestamp"),
            data.get("event_type"),
            data.get("workspace"),
            data.get("campaign_id"),
            data.get("unibox_url"),
            campaign_clean,
            data.get("email_account"),
            data.get("lead_email"),
            data.get("phone"),
            data.get("jobTitle"),
            data.get("companyName"),
            data.get("linkedIn"),
            data.get("step"),
            data.get("variant"),
            data.get("email"),
            cliente
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Evento '{data.get('event_type')}' guardado para {email}. Cliente: '{cliente}' Campaña: '{campaign_clean}'")

        asignar_evento_ajustado_por_email(email)

    except Exception as e:
        print("❌ Error guardando en PostgreSQL:", e)

@app.route('/webhook', methods=['POST'])
def receive_webhook():
    try:
        data = request.json
        cdmx_tz = pytz.timezone("America/Mexico_City")
        now = datetime.datetime.now(cdmx_tz).strftime('%Y-%m-%d %H:%M:%S')


        print(f"\n[📩 {now}] Webhook recibido:")
        print(json.dumps(data, indent=4, ensure_ascii=False))

        if data.get('event_type'):
            insertar_evento(data)
        else:
            print("⚠️ Webhook sin 'event_type', ignorado.")

        return {'status': 'ok'}, 200

    except Exception as e:
        print("❌ Error procesando webhook:", e)
        return {'status': 'error'}, 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

