import csv
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from collections import defaultdict
from datetime import datetime
import time

# Configuración
KEYSPACE = 'spotify_test'
CASSANDRA_HOSTS = ['127.0.0.1']
ARCHIVO_USUARIOS = 'usuarios.csv'
ARCHIVO_CANCIONES = 'canciones.csv'
ARCHIVO_ESCUCHAS = 'escuchas.csv'

# Conexión y creación del keyspace si no existe
def conectar_cassandra():
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    
    # Crear keyspace si no existe
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    
    session.set_keyspace(KEYSPACE)
    return session

# Crear tablas base y OLAP
def crear_tablas(session):
    tablas = {
        "usuarios": """
            CREATE TABLE IF NOT EXISTS usuarios (
                usuario_id int PRIMARY KEY,
                nombre text,
                ciudad text
            );
        """,
        "canciones": """
            CREATE TABLE IF NOT EXISTS canciones (
                cancion_id int PRIMARY KEY,
                titulo text,
                artista text,
                genero text
            );
        """,
        "escuchas": """
            CREATE TABLE IF NOT EXISTS escuchas (
                usuario_id int,
                fecha_escucha text,
                cancion_id int,
                PRIMARY KEY (usuario_id, fecha_escucha, cancion_id)
            );
        """,
        "tendencia_por_dia": """
            CREATE TABLE IF NOT EXISTS tendencia_por_dia (
                fecha text PRIMARY KEY,
                total_reproducciones int
            );
        """,
        "top_canciones_por_usuario": """
            CREATE TABLE IF NOT EXISTS top_canciones_por_usuario (
                id_usuario int,
                total_reproducciones int,
                id_cancion int,
                PRIMARY KEY (id_usuario, total_reproducciones, id_cancion)
            ) WITH CLUSTERING ORDER BY (total_reproducciones DESC, id_cancion ASC);
        """,
        "reproducciones_por_artista_mes": """
            CREATE TABLE IF NOT EXISTS reproducciones_por_artista_mes (
                artista text,
                mes text,
                reproducciones int,
                PRIMARY KEY (artista, mes)
            );
        """,
        "reproducciones_por_ciudad_genero": """
            CREATE TABLE IF NOT EXISTS reproducciones_por_ciudad_genero (
                ciudad text,
                genero text,
                reproducciones int,
                PRIMARY KEY (ciudad, genero)
            );
        """,
        "reproducciones_por_genero_mes": """
            CREATE TABLE IF NOT EXISTS reproducciones_por_genero_mes (
                genero text,
                mes text,
                reproducciones int,
                PRIMARY KEY (genero, mes)
            );
        """
    }

    for nombre, query in tablas.items():
        session.execute(query)
    print("✅ Todas las tablas creadas.")

# Carga de datos base
def cargar_usuarios(session):
    with open(ARCHIVO_USUARIOS, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            session.execute(
                "INSERT INTO usuarios (usuario_id, nombre, ciudad) VALUES (%s, %s, %s)",
                (int(row['usuario_id']), row['nombre'], row['ciudad'])
            )
    print("✅ Usuarios cargados.")

def cargar_canciones(session):
    with open(ARCHIVO_CANCIONES, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            session.execute(
                "INSERT INTO canciones (cancion_id, titulo, artista, genero) VALUES (%s, %s, %s, %s)",
                (int(row['cancion_id']), row['titulo'], row['artista'], row['genero'])
            )
    print("✅ Canciones cargadas.")

def cargar_escuchas(session):
    with open(ARCHIVO_ESCUCHAS, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            session.execute(
                "INSERT INTO escuchas (usuario_id, fecha_escucha, cancion_id) VALUES (%s, %s, %s)",
                (int(row['usuario_id']), row['fecha_escucha'], int(row['cancion_id']))
            )
    print("✅ Escuchas cargadas.")

# Procesar OLAP
def cargar_tablas_olap(session):
    usuarios = {}
    with open(ARCHIVO_USUARIOS, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            usuarios[int(row['usuario_id'])] = row['ciudad']

    canciones = {}
    with open(ARCHIVO_CANCIONES, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            canciones[int(row['cancion_id'])] = {'artista': row['artista'], 'genero': row['genero']}

    tendencia = defaultdict(int)
    genero_mes = defaultdict(int)
    artista_mes = defaultdict(int)
    ciudad_genero = defaultdict(int)
    canciones_por_usuario = defaultdict(lambda: defaultdict(int))

    with open(ARCHIVO_ESCUCHAS, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            user_id = int(row['usuario_id'])
            song_id = int(row['cancion_id'])
            fecha = row['fecha_escucha']
            ciudad = usuarios.get(user_id, 'desconocido')
            cancion_info = canciones.get(song_id, {'artista': 'desconocido', 'genero': 'desconocido'})
            artista = cancion_info['artista']
            genero = cancion_info['genero']
            mes = datetime.strptime(fecha, "%Y-%m-%d").strftime("%Y-%m")

            tendencia[fecha] += 1
            genero_mes[(genero, mes)] += 1
            artista_mes[(artista, mes)] += 1
            ciudad_genero[(ciudad, genero)] += 1
            canciones_por_usuario[user_id][song_id] += 1

    for fecha, count in tendencia.items():
        session.execute("INSERT INTO tendencia_por_dia (fecha, total_reproducciones) VALUES (%s, %s)", (fecha, count))

    for (genero, mes), count in genero_mes.items():
        session.execute("INSERT INTO reproducciones_por_genero_mes (genero, mes, reproducciones) VALUES (%s, %s, %s)", (genero, mes, count))

    for (artista, mes), count in artista_mes.items():
        session.execute("INSERT INTO reproducciones_por_artista_mes (artista, mes, reproducciones) VALUES (%s, %s, %s)", (artista, mes, count))

    for (ciudad, genero), count in ciudad_genero.items():
        session.execute("INSERT INTO reproducciones_por_ciudad_genero (ciudad, genero, reproducciones) VALUES (%s, %s, %s)", (ciudad, genero, count))

    for user_id, canciones_dict in canciones_por_usuario.items():
        for song_id, count in canciones_dict.items():
            session.execute("INSERT INTO top_canciones_por_usuario (id_usuario, total_reproducciones, id_cancion) VALUES (%s, %s, %s)", (user_id, count, song_id))

    print("✅ Tablas OLAP cargadas.")

# Ejecutar todo
def main():
    session = conectar_cassandra()
    crear_tablas(session)
    cargar_usuarios(session)
    cargar_canciones(session)
    cargar_escuchas(session)
    cargar_tablas_olap(session)
    print("🎉 Base de datos creada y cargada exitosamente.")

if __name__ == '__main__':
    main()