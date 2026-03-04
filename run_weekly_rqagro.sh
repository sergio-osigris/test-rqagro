#!/bin/bash

LOG_FILE="/home/ec2-user/supabase/rqagro_weekly.log"

echo -n "" > $LOG_FILE

echo "=============================" >> $LOG_FILE
echo "Inicio: $(date)" >> $LOG_FILE
echo "=============================" >> $LOG_FILE

cd /home/ec2-user/supabase || exit 1

echo "⚙️ Ejecutando insertar_datos_supabase.py..." >> $LOG_FILE
/usr/bin/python3 insertar_datos_supabase.py >> $LOG_FILE 2>&1

echo "Fin: $(date)" >> $LOG_FILE
echo "" >> $LOG_FILE

echo "⚙️ Subir log a s3..." >> $LOG_FILE
/usr/bin/python3 upload_log_s3.py >> $LOG_FILE 2>&1