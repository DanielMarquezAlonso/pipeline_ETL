import json
import psycopg2
import os

def generar_reporte():
    """Genera un reporte JSON con estadísticas del pipeline."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=5432,
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASS", "etl_secreto"),
        database=os.getenv("DB_NAME", "ventas")
    )
    cursor = conn.cursor()

    # Total de filas cargadas
    cursor.execute("SELECT COUNT(*) FROM ventas_limpias")
    total_filas = cursor.fetchone()[0]

    # Suma total de ventas
    cursor.execute("SELECT SUM(total) FROM ventas_limpias")
    total_ventas = float(cursor.fetchone()[0] or 0)

    # Ciudad con más ventas
    cursor.execute("SELECT ciudad, SUM(total) as t FROM ventas_limpias GROUP BY ciudad ORDER BY t DESC LIMIT 1")
    top_ciudad = cursor.fetchone()

    reporte = {
        "total_filas": total_filas,
        "total_ventas": round(total_ventas, 2),
        "top_ciudad": {"nombre": top_ciudad[0], "total": float(top_ciudad[1])},
    }

    conn.close()

    # Guardar reporte
    os.makedirs("./output", exist_ok=True)
    with open("./output/reporte.json", "w") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print(f"Reporte generado: {reporte}")

generar_reporte()