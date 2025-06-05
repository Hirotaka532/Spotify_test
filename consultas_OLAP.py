from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import threading
from concurrent.futures import ThreadPoolExecutor
import time

# Establecer conexión con configuración optimizada
cluster = Cluster(
    ['127.0.0.1'],
    # Optimizaciones de conexión
    load_balancing_policy=None,
    default_retry_policy=None,
    compression=True,
    protocol_version=4
)
session = cluster.connect('spotify_test')

# Configurar timeouts y fetch size para mejor rendimiento
session.default_timeout = 30
session.default_fetch_size = 1000  # Traer más registros por batch

# Cache en memoria para datos frecuentemente consultados
_cache = {}
_cache_timeout = 300  # 5 minutos

def get_from_cache(key):
    """Obtiene datos del cache si están disponibles y no han expirado."""
    if key in _cache:
        data, timestamp = _cache[key]
        if time.time() - timestamp < _cache_timeout:
            return data
    return None

def set_cache(key, data):
    """Guarda datos en el cache."""
    _cache[key] = (data, time.time())

def clear_cache():
    """Limpia el cache."""
    _cache.clear()

# Función optimizada para obtener nombres de usuario en lote
def obtener_nombres_usuarios_lote(usuario_ids):
    """
    Obtiene nombres de usuarios en lote para mejor rendimiento.
    """
    if not usuario_ids:
        return {}
    
    # Verificar cache primero
    cache_key = f"usuarios_{hash(tuple(sorted(usuario_ids)))}"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        # Preparar consulta
        stmt = session.prepare("SELECT usuario_id, nombre FROM usuarios WHERE usuario_id = ?")
        
        # Ejecutar consultas concurrentes
        futures = execute_concurrent_with_args(session, stmt, [(uid,) for uid in usuario_ids])
        
        resultado = {}
        for success, result in futures:
            if success and result:
                row = result[0]  # Primer resultado
                resultado[row.usuario_id] = row.nombre
            
        # Agregar usuarios no encontrados
        for uid in usuario_ids:
            if uid not in resultado:
                resultado[uid] = "Desconocido"
        
        # Guardar en cache
        set_cache(cache_key, resultado)
        return resultado
        
    except Exception as e:
        print(f"Error al obtener nombres de usuarios en lote: {e}")
        return {uid: "Desconocido" for uid in usuario_ids}

# Función optimizada para obtener títulos de canciones en lote
def obtener_titulos_canciones_lote(cancion_ids):
    """
    Obtiene títulos de canciones en lote para mejor rendimiento.
    """
    if not cancion_ids:
        return {}
    
    # Verificar cache primero
    cache_key = f"canciones_{hash(tuple(sorted(cancion_ids)))}"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        # Preparar consulta
        stmt = session.prepare("SELECT cancion_id, titulo FROM canciones WHERE cancion_id = ?")
        
        # Ejecutar consultas concurrentes
        futures = execute_concurrent_with_args(session, stmt, [(cid,) for cid in cancion_ids])
        
        resultado = {}
        for success, result in futures:
            if success and result:
                row = result[0]
                resultado[row.cancion_id] = row.titulo
            
        # Agregar canciones no encontradas
        for cid in cancion_ids:
            if cid not in resultado:
                resultado[cid] = "Desconocida"
        
        # Guardar en cache
        set_cache(cache_key, resultado)
        return resultado
        
    except Exception as e:
        print(f"Error al obtener títulos de canciones en lote: {e}")
        return {cid: "Desconocida" for cid in cancion_ids}

# Consulta optimizada: top canciones por usuario
def consultar_top_canciones_por_usuario_optimizado():
    """
    Consulta optimizada de canciones más reproducidas por usuario.
    """
    cache_key = "top_canciones_por_usuario"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        print("Ejecutando consulta optimizada: top canciones por usuario...")
        
        # Configurar fetch size para esta consulta
        session.default_fetch_size = 2000
        rows = session.execute("SELECT * FROM top_canciones_por_usuario")
        
        # Extraer IDs únicos
        usuario_ids = set()
        cancion_ids = set()
        datos_raw = []
        
        for row in rows:
            usuario_ids.add(row.id_usuario)
            cancion_ids.add(row.id_cancion)
            datos_raw.append({
                'id_usuario': row.id_usuario,
                'id_cancion': row.id_cancion,
                'reproducciones': row.total_reproducciones
            })
        
        print(f"Datos obtenidos: {len(datos_raw)} registros")
        
        # Obtener nombres en lote (más eficiente)
        nombres_usuarios = obtener_nombres_usuarios_lote(usuario_ids)
        titulos_canciones = obtener_titulos_canciones_lote(cancion_ids)
        
        print("Procesando y ordenando datos...")
        
        # Construir resultado final
        resultados = []
        for dato in datos_raw:
            nombre_usuario = nombres_usuarios.get(dato['id_usuario'], "Desconocido")
            usuario_mostrar = f"{dato['id_usuario']} - {nombre_usuario}"
            titulo_cancion = titulos_canciones.get(dato['id_cancion'], "Desconocida")
            
            resultados.append({
                'usuario': usuario_mostrar,
                'cancion': titulo_cancion,
                'reproducciones': dato['reproducciones']
            })
        
        # Ordenamiento optimizado usando key personalizada
        resultados.sort(key=lambda x: (x['usuario'].lower(), x['cancion'].lower()))
        
        print(f"Consulta completada: {len(resultados)} registros procesados")
        
        # Guardar en cache
        set_cache(cache_key, resultados)
        # Restaurar fetch size por defecto
        session.default_fetch_size = 1000
        return resultados
        
    except Exception as e:
        print(f"Error en consulta optimizada top canciones por usuario: {e}")
        return []

# Consulta optimizada: tendencia por día
def consultar_tendencia_por_dia_optimizado():
    """
    Consulta optimizada de tendencia por día.
    """
    cache_key = "tendencia_por_dia"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        print("Ejecutando consulta optimizada: tendencia por día...")
        
        # Configurar fetch size para esta consulta
        session.default_fetch_size = 5000
        rows = session.execute("SELECT * FROM tendencia_por_dia")
        
        resultados = []
        for row in rows:
            resultados.append({
                'fecha': row.fecha,
                'reproducciones': row.total_reproducciones
            })
        
        # Ordenamiento por fecha (más eficiente que en Cassandra)
        resultados.sort(key=lambda x: x['fecha'])
        
        print(f"Consulta completada: {len(resultados)} registros")
        
        # Guardar en cache
        set_cache(cache_key, resultados)
        # Restaurar fetch size por defecto
        session.default_fetch_size = 1000
        return resultados
        
    except Exception as e:
        print(f"Error en consulta optimizada tendencia por día: {e}")
        return []

# Consulta optimizada: reproducciones por artista por mes
def consultar_reproducciones_por_artista_mes_optimizado():
    """
    Consulta optimizada de reproducciones por artista y mes.
    """
    cache_key = "reproducciones_por_artista_mes"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        print("Ejecutando consulta optimizada: reproducciones por artista y mes...")
        
        # Configurar fetch size para esta consulta
        session.default_fetch_size = 3000
        rows = session.execute("SELECT * FROM reproducciones_por_artista_mes")
        
        resultados = []
        for row in rows:
            resultados.append({
                'artista': row.artista,
                'mes': row.mes,
                'reproducciones': row.reproducciones
            })
        
        # Ordenamiento optimizado con múltiples criterios
        resultados.sort(key=lambda x: (x['artista'].lower(), x['mes']))
        
        print(f"Consulta completada: {len(resultados)} registros")
        
        # Guardar en cache
        set_cache(cache_key, resultados)
        # Restaurar fetch size por defecto
        session.default_fetch_size = 1000
        return resultados
        
    except Exception as e:
        print(f"Error en consulta optimizada reproducciones por artista por mes: {e}")
        return []

# Consulta optimizada: reproducciones por género por mes
def consultar_reproducciones_por_genero_mes_optimizado():
    """
    Consulta optimizada de reproducciones por género y mes.
    """
    cache_key = "reproducciones_por_genero_mes"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        print("Ejecutando consulta optimizada: reproducciones por género y mes...")
        
        # Configurar fetch size para esta consulta
        session.default_fetch_size = 3000
        rows = session.execute("SELECT * FROM reproducciones_por_genero_mes")
        
        resultados = []
        for row in rows:
            resultados.append({
                'genero': row.genero,
                'mes': row.mes,
                'reproducciones': row.reproducciones
            })
        
        # Ordenamiento optimizado
        resultados.sort(key=lambda x: (x['genero'].lower(), x['mes']))
        
        print(f"Consulta completada: {len(resultados)} registros")
        
        # Guardar en cache
        set_cache(cache_key, resultados)
        # Restaurar fetch size por defecto
        session.default_fetch_size = 1000
        return resultados
        
    except Exception as e:
        print(f"Error en consulta optimizada reproducciones por género por mes: {e}")
        return []

# Consulta optimizada: reproducciones por ciudad y género
def consultar_reproducciones_por_ciudad_genero_optimizado():
    """
    Consulta optimizada de reproducciones por ciudad y género.
    """
    cache_key = "reproducciones_por_ciudad_genero"
    cached_result = get_from_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        print("Ejecutando consulta optimizada: reproducciones por ciudad y género...")
        
        # Configurar fetch size para esta consulta
        session.default_fetch_size = 3000
        rows = session.execute("SELECT * FROM reproducciones_por_ciudad_genero")
        
        resultados = []
        for row in rows:
            resultados.append({
                'ciudad': row.ciudad,
                'genero': row.genero,
                'reproducciones': row.reproducciones
            })
        
        # Ordenamiento optimizado
        resultados.sort(key=lambda x: (x['ciudad'].lower(), x['genero'].lower()))
        
        print(f"Consulta completada: {len(resultados)} registros")
        
        # Guardar en cache
        set_cache(cache_key, resultados)
        # Restaurar fetch size por defecto
        session.default_fetch_size = 1000
        return resultados
        
    except Exception as e:
        print(f"Error en consulta optimizada reproducciones por ciudad y género: {e}")
        return []

# Funciones auxiliares para obtener valores únicos (optimizadas)
def obtener_generos_unicos_optimizado():
    """Obtiene géneros únicos de forma optimizada."""
    try:
        datos = consultar_reproducciones_por_genero_mes_optimizado()
        generos = sorted(set(fila['genero'] for fila in datos))
        return generos
    except Exception as e:
        print(f"Error al obtener géneros únicos: {e}")
        return ["Todos"]

def obtener_artistas_unicos_optimizado():
    """Obtiene artistas únicos de forma optimizada."""
    try:
        datos = consultar_reproducciones_por_artista_mes_optimizado()
        artistas = sorted(set(fila['artista'] for fila in datos))
        return artistas
    except Exception as e:
        print(f"Error al obtener artistas únicos: {e}")
        return ["Todos"]

def obtener_ciudades_unicas_optimizado():
    """Obtiene ciudades únicas de forma optimizada."""
    try:
        datos = consultar_reproducciones_por_ciudad_genero_optimizado()
        ciudades = sorted(set(fila['ciudad'] for fila in datos))
        return ciudades
    except Exception as e:
        print(f"Error al obtener ciudades únicas: {e}")
        return ["Todos"]

def obtener_meses_unicos_optimizado():
    """Obtiene meses únicos de forma optimizada."""
    try:
        datos = consultar_reproducciones_por_genero_mes_optimizado()
        meses_raw = sorted(set(fila['mes'] for fila in datos))
        return meses_raw
    except Exception as e:
        print(f"Error al obtener meses únicos: {e}")
        return ["2024-01"]

# Función para limpiar cache manualmente
def limpiar_cache():
    """Limpia el cache de consultas."""
    clear_cache()
    print("Cache limpiado")

# Funciones de compatibilidad con la interfaz original
def consultar_reproducciones_por_genero_mes():
    return consultar_reproducciones_por_genero_mes_optimizado()

def consultar_reproducciones_por_artista_mes():
    return consultar_reproducciones_por_artista_mes_optimizado()

def consultar_reproducciones_por_ciudad_genero():
    return consultar_reproducciones_por_ciudad_genero_optimizado()

def consultar_top_canciones_por_usuario():
    return consultar_top_canciones_por_usuario_optimizado()

def consultar_tendencia_por_dia():
    return consultar_tendencia_por_dia_optimizado()

# Función para pre-cargar datos en background
def precargar_datos_background():
    """
    Pre-carga datos en background para mejorar la experiencia del usuario.
    """
    def cargar():
        try:
            print("Pre-cargando datos en background...")
            consultar_reproducciones_por_genero_mes_optimizado()
            consultar_reproducciones_por_artista_mes_optimizado()
            consultar_reproducciones_por_ciudad_genero_optimizado()
            print("Pre-carga completada")
        except Exception as e:
            print(f"Error en pre-carga: {e}")
    
    # Ejecutar en thread separado
    thread = threading.Thread(target=cargar, daemon=True)
    thread.start()

# Inicializar pre-carga al importar el módulo
if __name__ != "__main__":
    precargar_datos_background()