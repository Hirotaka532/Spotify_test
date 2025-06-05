import customtkinter as ctk
from PIL import Image
import consultas_OLAP as olap
import threading
import time

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

root = ctk.CTk()
root.title("Sistema de Recomendación Musical - Análisis OLAP")
root.geometry("900x700")

# Diccionario para convertir meses
MESES_NOMBRES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
}

def convertir_mes_a_nombre(fecha_mes):
    try:
        año, mes = fecha_mes.split('-')
        nombre_mes = MESES_NOMBRES.get(mes, mes)
        return f"{nombre_mes} {año}"
    except:
        return fecha_mes

# Variables globales para almacenar datos originales
datos_originales_cache = {}

def obtener_generos_unicos():
    try:
        return olap.obtener_generos_unicos_optimizado()
    except Exception as e:
        print(f"Error al obtener géneros únicos: {e}")
        return ["Todos"]

def obtener_artistas_unicos():
    try:
        return olap.obtener_artistas_unicos_optimizado()
    except Exception as e:
        print(f"Error al obtener artistas únicos: {e}")
        return ["Todos"]

def obtener_ciudades_unicas():
    try:
        return olap.obtener_ciudades_unicas_optimizado()
    except Exception as e:
        print(f"Error al obtener ciudades únicas: {e}")
        return ["Todos"]

def obtener_meses_unicos():
    try:
        meses_raw = olap.obtener_meses_unicos_optimizado()
        meses_convertidos = [(mes, convertir_mes_a_nombre(mes)) for mes in meses_raw]
        return meses_convertidos
    except Exception as e:
        print(f"Error al obtener meses únicos: {e}")
        return [("2024-01", "Enero 2024")]

# Frame principal (pantalla de inicio)
frame_principal = ctk.CTkFrame(root)
frame_principal.pack(fill="both", expand=True)

# Frame de resultados (tabla)
frame_resultado = ctk.CTkFrame(root)

# Variables para los filtros
filtro_genero = ctk.StringVar(value="Todos")
filtro_artista = ctk.StringVar(value="Todos")
filtro_ciudad = ctk.StringVar(value="Todos")
filtro_mes = ctk.StringVar(value="Todos")

def cargar_datos_async(consulta_func, callback, *args):
    """
    Carga datos de forma asíncrona para no bloquear la UI.
    """
    def worker():
        try:
            start_time = time.time()
            datos = consulta_func(*args)
            end_time = time.time()
            print(f"Consulta completada en {end_time - start_time:.2f} segundos")
            
            # Llamar callback en el hilo principal
            root.after(0, lambda: callback(datos))
        except Exception as e:
            print(f"Error en carga asíncrona: {e}")
            root.after(0, lambda: callback([]))
    
    # Ejecutar en thread separado
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

def mostrar_tabla_con_filtros_limpia(datos, columnas, titulo, tipo_consulta=None):
    # Guardar datos originales para filtros
    datos_originales_cache[tipo_consulta] = datos
    
    frame_principal.pack_forget()
    for widget in frame_resultado.winfo_children():
        widget.destroy()
    frame_resultado.pack(fill="both", expand=True)

    # Título con información de rendimiento
    titulo_completo = f"{titulo} ({len(datos)} registros)"
    ctk.CTkLabel(frame_resultado, text=titulo_completo, font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(10, 0))

    # Frame para filtros
    if tipo_consulta:
        frame_filtros = ctk.CTkFrame(frame_resultado)
        frame_filtros.pack(padx=20, pady=(10, 10), fill="x")
        crear_filtros_reactivos(frame_filtros, tipo_consulta, datos, columnas, titulo)

    # Contenedor principal para la tabla con scroll optimizado
    contenedor_tabla = ctk.CTkFrame(frame_resultado)
    contenedor_tabla.pack(padx=20, pady=10, fill="both", expand=True)

    # Usar CTkScrollableFrame con configuración optimizada
    tabla_scrollable = ctk.CTkScrollableFrame(
        contenedor_tabla, 
        width=500, 
        height=400,
        scrollbar_button_color=("gray70", "gray30"),
        scrollbar_button_hover_color=("gray60", "gray40")
    )
    tabla_scrollable.pack(fill="both", expand=True, padx=10, pady=10)

    # Mapeo de nombres de columnas a claves de diccionario
    column_key_map = {
        "Género": "genero",
        "Mes": "mes",
        "Reproducciones": "reproducciones",
        "Artista": "artista",
        "Ciudad": "ciudad",
        "Usuario ID": "usuario",
        "Canción": "cancion",
        "Día": "fecha"
    }

    # Función optimizada para actualizar los datos de la tabla
    def actualizar_tabla_datos(nuevos_datos, cols):
        # Limpiar tabla actual de forma eficiente
        for widget in tabla_scrollable.winfo_children():
            widget.destroy()
        
        if not nuevos_datos:
            ctk.CTkLabel(tabla_scrollable, text="No hay datos para mostrar", 
                        font=ctk.CTkFont(size=14)).grid(row=0, column=0, pady=20)
            return

        # Crear encabezados con mejor styling
        for col_idx, display_name in enumerate(cols):
            label = ctk.CTkLabel(
                tabla_scrollable, 
                text=display_name, 
                font=ctk.CTkFont(weight="bold", size=12),
                fg_color=("gray70", "gray30"), 
                corner_radius=5, 
                width=150,
                height=30
            )
            label.grid(row=0, column=col_idx, padx=3, pady=3, sticky="ew")

        # Crear filas de datos de forma optimizada
        for fila_idx, registro in enumerate(nuevos_datos, start=1):
            for col_idx, display_name in enumerate(cols):
                key = column_key_map.get(display_name, display_name.lower().replace(" ", "_"))
                valor = registro.get(key)

                # Conversión optimizada de mes
                if display_name == "Mes" and isinstance(valor, str) and len(valor) == 7 and '-' in valor:
                    valor_mostrar = convertir_mes_a_nombre(valor)
                else:
                    valor_mostrar = str(valor) if valor is not None else ""

                # Colores alternados optimizados
                color_fila = ("gray90", "gray20") if fila_idx % 2 == 0 else ("white", "gray25")

                label = ctk.CTkLabel(
                    tabla_scrollable, 
                    text=valor_mostrar,
                    fg_color=color_fila, 
                    corner_radius=3, 
                    width=150,
                    height=25,
                    font=ctk.CTkFont(size=11)
                )
                label.grid(row=fila_idx, column=col_idx, padx=3, pady=1, sticky="ew")

        # Configurar el peso de las columnas
        for col in range(len(cols)):
            tabla_scrollable.grid_columnconfigure(col, weight=1)
        
        # Actualizar título con nuevo conteo
        titulo_actualizado = f"{titulo} ({len(nuevos_datos)} registros)"
        for widget in frame_resultado.winfo_children():
            if isinstance(widget, ctk.CTkLabel) and titulo in widget.cget("text"):
                widget.configure(text=titulo_actualizado)
                break

    # Mostrar datos iniciales
    actualizar_tabla_datos(datos, columnas)

    # Botón volver
    ctk.CTkButton(frame_resultado, text="🔙 Volver al Menú", command=volver_menu, width=200).pack(pady=20)

def crear_filtros_reactivos(frame_filtros, tipo_consulta, datos_originales, columnas, titulo):
    """
    Crea filtros reactivos que se aplican automáticamente.
    """
    def aplicar_filtros_automatico():
        """Aplica filtros automáticamente cuando cambian los valores."""
        datos_filtrados = datos_originales.copy()
        
        if tipo_consulta == "genero_mes":
            genero_seleccionado = filtro_genero.get()
            mes_seleccionado = filtro_mes.get()
            
            if genero_seleccionado != "Todos":
                datos_filtrados = [fila for fila in datos_filtrados if fila['genero'] == genero_seleccionado]
            
            if mes_seleccionado != "Todos":
                # Encontrar el mes raw correspondiente
                meses_raw = sorted(set(fila['mes'] for fila in datos_originales))
                mes_raw = None
                for mes in meses_raw:
                    if convertir_mes_a_nombre(mes) == mes_seleccionado:
                        mes_raw = mes
                        break
                if mes_raw:
                    datos_filtrados = [fila for fila in datos_filtrados if fila['mes'] == mes_raw]
        
        elif tipo_consulta == "artista_mes":
            artista_seleccionado = filtro_artista.get()
            mes_seleccionado = filtro_mes.get()
            
            if artista_seleccionado != "Todos":
                datos_filtrados = [fila for fila in datos_filtrados if fila['artista'] == artista_seleccionado]
            
            if mes_seleccionado != "Todos":
                meses_raw = sorted(set(fila['mes'] for fila in datos_originales))
                mes_raw = None
                for mes in meses_raw:
                    if convertir_mes_a_nombre(mes) == mes_seleccionado:
                        mes_raw = mes
                        break
                if mes_raw:
                    datos_filtrados = [fila for fila in datos_filtrados if fila['mes'] == mes_raw]
        
        elif tipo_consulta == "ciudad_genero":
            ciudad_seleccionada = filtro_ciudad.get()
            genero_seleccionado = filtro_genero.get()
            
            if ciudad_seleccionada != "Todos":
                datos_filtrados = [fila for fila in datos_filtrados if fila['ciudad'] == ciudad_seleccionada]
            
            if genero_seleccionado != "Todos":
                datos_filtrados = [fila for fila in datos_filtrados if fila['genero'] == genero_seleccionado]
        
        # Actualizar tabla inmediatamente
        mostrar_tabla_con_filtros_limpia(datos_filtrados, columnas, titulo, tipo_consulta)

    def limpiar_todos_los_filtros():
        """Limpia todos los filtros y recarga todos los datos originales desde la base de datos."""
        # Resetear todas las variables de filtro
        filtro_genero.set("Todos")
        filtro_artista.set("Todos")
        filtro_ciudad.set("Todos")
        filtro_mes.set("Todos")
        
        # Limpiar cache para forzar recarga desde base de datos
        olap.limpiar_cache()
        
        # Recargar datos según el tipo de consulta
        if tipo_consulta == "genero_mes":
            cargar_datos_async(olap.consultar_reproducciones_por_genero_mes_optimizado, 
                         lambda d: mostrar_tabla_con_filtros_limpia(d, columnas, titulo, tipo_consulta))
        elif tipo_consulta == "artista_mes":
            cargar_datos_async(olap.consultar_reproducciones_por_artista_mes_optimizado,
                         lambda d: mostrar_tabla_con_filtros_limpia(d, columnas, titulo, tipo_consulta))
        elif tipo_consulta == "ciudad_genero":
            cargar_datos_async(olap.consultar_reproducciones_por_ciudad_genero_optimizado,
                         lambda d: mostrar_tabla_con_filtros_limpia(d, columnas, titulo, tipo_consulta))

    if tipo_consulta == "genero_mes":
        # Filtro de género
        ctk.CTkLabel(frame_filtros, text="Filtrar por Género:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=10, sticky="w")
        generos = ["Todos"] + sorted(set(fila['genero'] for fila in datos_originales))
        combo_genero = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_genero, 
            values=generos, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_genero.grid(row=0, column=1, padx=10, pady=10)

        # Filtro de mes
        ctk.CTkLabel(frame_filtros, text="Filtrar por Mes:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=2, padx=10, pady=10, sticky="w")
        meses_raw = sorted(set(fila['mes'] for fila in datos_originales))
        meses_nombres = ["Todos"] + [convertir_mes_a_nombre(mes) for mes in meses_raw]
        combo_mes = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_mes, 
            values=meses_nombres, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_mes.grid(row=0, column=3, padx=10, pady=10)

        # Botón limpiar filtros (reemplazar el botón existente)
        ctk.CTkButton(
            frame_filtros, 
            text="🔄 Limpiar Filtros", 
            command=limpiar_todos_los_filtros, 
            width=130,
            fg_color=("#3B8ED0", "#1E5BA8"),  # Azul más prominente
            hover_color=("#2B7BC0", "#1E4A98")
        ).grid(row=0, column=4, padx=20, pady=10)

    elif tipo_consulta == "artista_mes":
        # Filtro de artista
        ctk.CTkLabel(frame_filtros, text="Filtrar por Artista:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=10, sticky="w")
        artistas = ["Todos"] + sorted(set(fila['artista'] for fila in datos_originales))
        combo_artista = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_artista, 
            values=artistas, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_artista.grid(row=0, column=1, padx=10, pady=10)

        ctk.CTkLabel(frame_filtros, text="Filtrar por Mes:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=2, padx=10, pady=10, sticky="w")
        meses_raw = sorted(set(fila['mes'] for fila in datos_originales))
        meses_nombres = ["Todos"] + [convertir_mes_a_nombre(mes) for mes in meses_raw]
        combo_mes = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_mes, 
            values=meses_nombres, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_mes.grid(row=0, column=3, padx=10, pady=10)

        # Botón limpiar filtros (reemplazar el botón existente)
        ctk.CTkButton(
            frame_filtros, 
            text="🔄 Limpiar Filtros", 
            command=limpiar_todos_los_filtros, 
            width=130,
            fg_color=("#3B8ED0", "#1E5BA8"),  # Azul más prominente
            hover_color=("#2B7BC0", "#1E4A98")
        ).grid(row=0, column=4, padx=20, pady=10)

    elif tipo_consulta == "ciudad_genero":
        # Filtro de ciudad
        ctk.CTkLabel(frame_filtros, text="Filtrar por Ciudad:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=10, sticky="w")
        ciudades = ["Todos"] + sorted(set(fila['ciudad'] for fila in datos_originales))
        combo_ciudad = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_ciudad, 
            values=ciudades, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_ciudad.grid(row=0, column=1, padx=10, pady=10)

        ctk.CTkLabel(frame_filtros, text="Filtrar por Género:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=2, padx=10, pady=10, sticky="w")
        generos = ["Todos"] + sorted(set(fila['genero'] for fila in datos_originales))
        combo_genero = ctk.CTkOptionMenu(
            frame_filtros, 
            variable=filtro_genero, 
            values=generos, 
            width=150,
            command=lambda x: aplicar_filtros_automatico()
        )
        combo_genero.grid(row=0, column=3, padx=10, pady=10)

        # Botón limpiar filtros (reemplazar el botón existente)
        ctk.CTkButton(
            frame_filtros, 
            text="🔄 Limpiar Filtros", 
            command=limpiar_todos_los_filtros, 
            width=130,
            fg_color=("#3B8ED0", "#1E5BA8"),  # Azul más prominente
            hover_color=("#2B7BC0", "#1E4A98")
        ).grid(row=0, column=4, padx=20, pady=10)

def mostrar_tabla_simple_limpia(datos, columnas, titulo):
    """
    Muestra tabla simple sin filtros pero optimizada.
    """
    frame_principal.pack_forget()
    for widget in frame_resultado.winfo_children():
        widget.destroy()
    frame_resultado.pack(fill="both", expand=True)
    
    titulo_completo = f"{titulo} ({len(datos)} registros)"
    ctk.CTkLabel(frame_resultado, text=titulo_completo, font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(10, 0))
    
    # Contenedor de tabla optimizado
    contenedor_tabla = ctk.CTkFrame(frame_resultado)
    contenedor_tabla.pack(padx=20, pady=10, fill="both", expand=True)
    
    tabla_scrollable = ctk.CTkScrollableFrame(contenedor_tabla, width=500, height=400)
    tabla_scrollable.pack(fill="both", expand=True, padx=10, pady=10)
    
    column_key_map = {
        "Género": "genero",
        "Mes": "mes",
        "Reproducciones": "reproducciones",
        "Artista": "artista",
        "Ciudad": "ciudad",
        "Usuario ID": "usuario",
        "Canción": "cancion",
        "Día": "fecha"
    }
    
    # Encabezados
    for col_idx, display_name in enumerate(columnas):
        label = ctk.CTkLabel(tabla_scrollable, text=display_name, font=ctk.CTkFont(weight="bold"),
                             fg_color=("gray70", "gray30"), corner_radius=5, width=150)
        label.grid(row=0, column=col_idx, padx=5, pady=5, sticky="ew")
    
    # Datos
    for fila_idx, registro in enumerate(datos, start=1):
        for col_idx, display_name in enumerate(columnas):
            key = column_key_map.get(display_name, display_name.lower().replace(" ", "_"))
            valor = registro.get(key)
            valor_mostrar = str(valor) if valor is not None else ""
            
            color_fila = ("gray90", "gray20") if fila_idx % 2 == 0 else ("white", "gray25")
            
            label = ctk.CTkLabel(tabla_scrollable, text=valor_mostrar,
                                 fg_color=color_fila, corner_radius=3, width=150)
            label.grid(row=fila_idx, column=col_idx, padx=5, pady=2, sticky="ew")
    
    for col in range(len(columnas)):
        tabla_scrollable.grid_columnconfigure(col, weight=1)
    
    ctk.CTkButton(frame_resultado, text="🔙 Volver al Menú", command=volver_menu, width=200).pack(pady=20)

def volver_menu():
    frame_resultado.pack_forget()
    frame_principal.pack(fill="both", expand=True)
    # Resetear filtros
    filtro_genero.set("Todos")
    filtro_artista.set("Todos")
    filtro_ciudad.set("Todos")
    filtro_mes.set("Todos")
    # Limpiar cache de datos originales
    datos_originales_cache.clear()

# ------------------------
# Contenido del Frame Principal
# ------------------------

# Logo
try:
    image = ctk.CTkImage(Image.open("logo.png"), size=(320, 200))
    ctk.CTkLabel(frame_principal, image=image, text="").pack(pady=(10, 0))
except:
    pass

ctk.CTkLabel(
    frame_principal,
    text="Sistema de Recomendación Musical - SpotiTest",
    font=ctk.CTkFont(size=20, weight="bold")
).pack(pady=10)

color_consulta = "#2d5f39"
hover_consulta = "#264d30"
fuente_boton = ctk.CTkFont(size=14, weight="bold")

# Funciones conectadas a cada botón (sin pantalla de carga)
def reproducciones_por_genero_mes():
    cargar_datos_async(
        olap.consultar_reproducciones_por_genero_mes_optimizado,
        lambda datos: mostrar_tabla_con_filtros_limpia(
            datos, ["Género", "Mes", "Reproducciones"],
            "🎧 Reproducciones por Género y Mes", "genero_mes"
        )
    )

def reproducciones_por_artista_mes():
    cargar_datos_async(
        olap.consultar_reproducciones_por_artista_mes_optimizado,
        lambda datos: mostrar_tabla_con_filtros_limpia(
            datos, ["Artista", "Mes", "Reproducciones"],
            "🎤 Reproducciones por Artista y Mes", "artista_mes"
        )
    )

def reproducciones_por_ciudad_genero():
    cargar_datos_async(
        olap.consultar_reproducciones_por_ciudad_genero_optimizado,
        lambda datos: mostrar_tabla_con_filtros_limpia(
            datos, ["Ciudad", "Género", "Reproducciones"],
            "🌍 Reproducciones por Ciudad y Género", "ciudad_genero"
        )
    )

def top_canciones_por_usuario():
    cargar_datos_async(
        olap.consultar_top_canciones_por_usuario_optimizado,
        lambda datos: mostrar_tabla_simple_limpia(
            datos, ["Usuario ID", "Canción", "Reproducciones"],
            "👤 Top Canciones por Usuario"
        )
    )

def tendencia_por_dia():
    cargar_datos_async(
        olap.consultar_tendencia_por_dia_optimizado,
        lambda datos: mostrar_tabla_simple_limpia(
            datos, ["Día", "Reproducciones"],
            "📈 Reproducciones por Día"
        )
    )

def salir_app():
    root.destroy()

# Botones principales
ctk.CTkButton(frame_principal, text="🎧 Reproducciones Totales por Género (Mes)", command=reproducciones_por_genero_mes, width=430, fg_color=color_consulta, hover_color=hover_consulta, font=fuente_boton).pack(pady=10)
ctk.CTkButton(frame_principal, text="🎤 Reproducciones Totales por Artista (Mes)", command=reproducciones_por_artista_mes, width=430, fg_color=color_consulta, hover_color=hover_consulta, font=fuente_boton).pack(pady=10)
ctk.CTkButton(frame_principal, text="🌍 Reproducciones por Ciudad y Género", command=reproducciones_por_ciudad_genero, width=430, fg_color=color_consulta, hover_color=hover_consulta, font=fuente_boton).pack(pady=10)
ctk.CTkButton(frame_principal, text="👤 Canciones más escuchadas por Usuario (Último mes)", command=top_canciones_por_usuario, width=430, fg_color=color_consulta, hover_color=hover_consulta, font=fuente_boton).pack(pady=10)
ctk.CTkButton(frame_principal, text="📈 Total de Reproducciones por Día", command=tendencia_por_dia, width=430, fg_color=color_consulta, hover_color=hover_consulta, font=fuente_boton).pack(pady=10)
ctk.CTkButton(frame_principal, text="❌ Salir", command=salir_app, width=430, fg_color="red", hover_color="darkred", font=fuente_boton).pack(pady=20)

# Mostrar menú principal al iniciar
frame_principal.pack(fill="both", expand=True)

# Iniciar aplicación
if __name__ == "__main__":
    root.mainloop()