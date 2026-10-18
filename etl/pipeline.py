# Archivo: etl/pipeline.py
"""Mini pipeline ETL: CSV → limpieza → PostgreSQL"""
import csv
import psycopg2
from datetime import datetime
import time
import os

def esperar_postgres(max_intentos=10):
    """Espera a que PostgreSQL esté listo antes de conectar."""
    for intento in range(max_intentos):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),
                port=5432,
                user=os.getenv("DB_USER", "etl_user"),
                password=os.getenv("DB_PASS", "etl_secreto"),
                database=os.getenv("DB_NAME", "ventas")
            )
            conn.close()
            print(f"[OK] PostgreSQL listo (intento {intento + 1})")
            return True
        except psycopg2.OperationalError:
            print(f"⏳ Esperando PostgreSQL... (intento {intento + 1})")
            time.sleep(2)
    raise Exception("[ERROR] PostgreSQL no respondió después de varios intentos")

def leer_csv(filepath):
    """Lee el CSV crudo."""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def limpiar_fila(fila):
    """Limpia y valida una fila. Retorna None si no es válida."""
    # Validar cantidad
    try:
        cantidad = int(fila['cantidad'])
        if cantidad <= 0:
            print(f"  [AVISO]  Fila {fila['id']}: cantidad inválida ({cantidad})")
            return None
    except (ValueError, TypeError):
        print(f"  [AVISO]  Fila {fila['id']}: cantidad vacía o no numérica")
        return None

    # Validar cliente
    cliente = fila.get('cliente', '').strip() or 'Desconocido'

    # Normalizar fecha (soportar YYYY-MM-DD y DD/MM/YYYY)
    fecha_raw = fila['fecha'].strip()
    try:
        if '/' in fecha_raw:
            fecha = datetime.strptime(fecha_raw, '%d/%m/%Y').date()
        else:
            fecha = datetime.strptime(fecha_raw, '%Y-%m-%d').date()
    except ValueError:
        print(f"  [AVISO]  Fila {fila['id']}: fecha inválida ({fecha_raw})")
        return None

    # Validar precio
    try:
        precio = float(fila['precio_unitario'])
        if precio <= 0:
            print(f"  [AVISO]  Fila {fila['id']}: precio inválido ({precio})")
            return None
    except (ValueError, TypeError):
        print(f"  [AVISO]  Fila {fila['id']}: precio vacío o no numérico")
        return None

    total = cantidad * precio

    return {
        'producto': fila['producto'].strip(),
        'cantidad': cantidad,
        'precio_unitario': precio,
        'total': round(total, 2),
        'fecha': fecha,
        'cliente': cliente,
        'ciudad': fila['ciudad'].strip(),
    }

def cargar_en_postgres(filas_limpias):
    """Inserta las filas limpias en PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=5432,
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASS", "etl_secreto"),
        database=os.getenv("DB_NAME", "ventas")
    )
    cursor = conn.cursor()

    for fila in filas_limpias:
        cursor.execute("""
            INSERT INTO ventas_limpias
            (producto, cantidad, precio_unitario, total, fecha, cliente, ciudad)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            fila['producto'], fila['cantidad'], fila['precio_unitario'],
            fila['total'], fila['fecha'], fila['cliente'], fila['ciudad']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    return len(filas_limpias)

if __name__ == '__main__':
    print("=" * 50)
    print("Pipeline ETL iniciado")
    print("=" * 50)

    esperar_postgres()

    # EXTRACT
    print("\nExtrayendo datos del CSV...")
    datos_crudos = leer_csv('/data/data_raw.csv')
    print(f"   Filas leídas: {len(datos_crudos)}")

    # TRANSFORM
    print("\nLimpiando y transformando...")
    datos_limpios = []
    for fila in datos_crudos:
        limpia = limpiar_fila(fila)
        if limpia:
            datos_limpios.append(limpia)

    print(f"   Filas válidas: {len(datos_limpios)}/{len(datos_crudos)}")

    # LOAD
    print("\nCargando en PostgreSQL...")
    insertadas = cargar_en_postgres(datos_limpios)
    print(f"   Filas insertadas: {insertadas}")

    print("\n[OK] Pipeline completado exitosamente!")
    print("=" * 50)