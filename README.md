# 🎵 Sistema de Recomendación Musical con OLAP en Apache Cassandra

Este proyecto implementa un sistema de análisis de datos musicales usando OLAP sobre Apache Cassandra. 
Incluye una interfaz gráfica desarrollada con `customtkinter` para explorar visualmente los resultados.

---

## 📁 Estructura del proyecto

```
├── datos/                  # Archivos CSV con datos de usuarios, canciones y escuchas
├── img/                    # Logo de la aplicación
├── montar_bd_completa.py   # Script para crear keyspace, tablas y cargar datos
├── spotify_test_GUI.py     # Interfaz gráfica para realizar consultas OLAP
├── consultas_OLAP.py       # Módulo con consultas optimizadas
├── requirements.txt        # Dependencias del proyecto
```

---

## 🚀 Requisitos

- Python 3.9 o superior
- Apache Cassandra instalado y corriendo en `127.0.0.1`
- Los archivos `.csv` deben estar en la carpeta `datos/`

---

## ⚙️ Instalación

1. Clona este repositorio:

```bash
git clone https://github.com/tu_usuario/spotify-olap.git
cd spotify-olap
```

2. Instala las dependencias de Python:

```bash
pip install -r requirements.txt
```

---

## 🛠️ Inicialización de la Base de Datos

Ejecuta el siguiente script para:

- Crear el keyspace `spotify_test`
- Crear todas las tablas
- Cargar los datos desde los archivos CSV

```bash
python montar_bd_completa.py
```

---

## 🖥️ Ejecutar la Aplicación

Ejecuta la interfaz gráfica para explorar los datos con filtros OLAP:

```bash
python spotify_test_GUI.py
```

---

## 📷 Vista Previa

![Logo](img/logo.png)

---

## 📌 Créditos

Proyecto desarrollado para la asignatura **Sistemas de Bases de Datos II**  
Universidad Nacional Experimental de Guayana – Ingeniería Informática  
Autores: *[tu nombre y/o grupo]*  
