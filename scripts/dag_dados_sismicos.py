from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import hashlib
from sqlalchemy import create_engine, text

def obter_motor_dw():
    """Busca as credenciais dinamicamente no cofre do Airflow."""
    credenciais = BaseHook.get_connection('banco_data_warehouse')
    url = f"postgresql+psycopg2://{credenciais.login}:{credenciais.password}@{credenciais.host}:{credenciais.port}/{credenciais.schema}"
    return create_engine(url, isolation_level="AUTOCOMMIT")

def gerar_hash_local(row):
    """Função auxiliar para gerar o ID único do local usando as colunas do Pandas."""
    chave = f"{row['nome_local']}_{row['latitude']}_{row['longitude']}"
    return hashlib.md5(chave.encode()).hexdigest()

def extrair_transformar_carregar_sismos():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    print("Conectando à API do USGS...")
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Falha na extração. Código: {response.status_code}")
        return

    # 1. CARGA TOTAL (O TABELÃO)
    # json_normalize achata o JSON aninhado em um único DataFrame gigante
    features = response.json()['features']
    df_mestre = pd.json_normalize(features)
    
    # 2. LIMPEZA E TRATAMENTO VETORIZADO NO PANDAS
    # Renomeando as colunas que vieram achatadas para nomes mais fáceis
    df_mestre.rename(columns={
        'id': 'id_evento',
        'properties.mag': 'magnitude',
        'properties.place': 'nome_local'
    }, inplace=True)
    
    # Tratando o Tempo
    # Converte os milissegundos para datetime do Pandas
    df_mestre[['distancia_do_nome_local', 'nome_local']] = df_mestre['nome_local'].str.extract(r'(\d+)\s*km\s*(.*)')
    df_mestre['data_hora'] = pd.to_datetime(df_mestre['properties.time'], unit='ms')
    df_mestre['id_tempo'] = df_mestre['data_hora'].dt.strftime('%Y%m%d%H%M%S') 
    df_mestre['data_completa'] = df_mestre['data_hora'].dt.date
    df_mestre['hora_completa'] = df_mestre['data_hora'].dt.time
    df_mestre['ano'] = df_mestre['data_hora'].dt.year
    df_mestre['mes'] = df_mestre['data_hora'].dt.month
    df_mestre['dia'] = df_mestre['data_hora'].dt.day
    
    # Tratando a Localização e Geometria
    # O campo coordinates vem como uma lista [longitude, latitude, profundidade]
    df_mestre['longitude'] = df_mestre['geometry.coordinates'].apply(lambda x: x[0] if isinstance(x, list) else None)
    df_mestre['latitude'] = df_mestre['geometry.coordinates'].apply(lambda x: x[1] if isinstance(x, list) else None)
    df_mestre['profundidade_km'] = df_mestre['geometry.coordinates'].apply(lambda x: x[2] if isinstance(x, list) else None)
    
    # Criando o Hash (ID Local) aplicando a função em cada linha (axis=1)
    df_mestre['id_localizacao'] = df_mestre.apply(gerar_hash_local, axis=1)
    
    
    # 3. RECORTANDO O TABELÃO NOS 3 DATAFRAMES FINAIS
    print(f"Dados brutos tratados. Total de registros: {len(df_mestre)}")
    
    # DataFrame: Dimensão Tempo
    df_tempo = df_mestre[['id_tempo', 'data_completa', 'hora_completa', 'ano', 'mes', 'dia']].drop_duplicates(subset=['id_tempo'])
    
    # DataFrame: Dimensão Local
    df_local = df_mestre[['id_localizacao', 'nome_local', 'distancia_do_nome_local', 'latitude', 'longitude']].drop_duplicates(subset=['id_localizacao'])
    
    # DataFrame: Tabela Fato
    df_fato = df_mestre[['id_evento', 'id_localizacao', 'id_tempo', 'magnitude', 'profundidade_km']].drop_duplicates(subset=['id_evento'])
    
    
    # 4. CARGA NO BANCO DE DADOS (Inalterado)
    engine = create_engine(obter_motor_dw().url)
    with engine.begin() as conn:
        for row in df_tempo.to_dict('records'):
            conn.execute(text("""
                INSERT INTO sismos.tempo (id_tempo, data_completa, hora_completa, ano, mes, dia)
                VALUES (:id_tempo, :data_completa, :hora_completa, :ano, :mes, :dia)
                ON CONFLICT (id_tempo) DO NOTHING;
            """), row)
            
        for row in df_local.to_dict('records'):
            conn.execute(text("""
                INSERT INTO sismos.localizacao (id_localizacao, nome_local, distancia_do_nome_local, latitude, longitude)
                VALUES (:id_localizacao, :nome_local, :distancia_do_nome_local, :latitude, :longitude)
                ON CONFLICT (id_localizacao) DO NOTHING;
            """), row)
            
        for row in df_fato.to_dict('records'):
            conn.execute(text("""
                INSERT INTO sismos.fato_tremor (id_evento, id_local, id_tempo, magnitude, profundidade_km)
                VALUES (:id_evento, :id_localizacao, :id_tempo, :magnitude, :profundidade_km)
                ON CONFLICT (id_evento) DO NOTHING;
            """), row)
            
    print("ETL concluído. Dados separados e carregados com sucesso!")

# --- ORQUESTRAÇÃO AIRFLOW ---
with DAG(
    'etl_sismos_usgs_v2',
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=2)},
    schedule_interval=timedelta(hours=4),
    start_date=datetime(2026, 5, 29),
    catchup=False,
    description='Extrai sismos e recorta DataFrames',
    tags=['api', 'sismos', 'pandas'],
) as dag:

    task_popular_sismos = PythonOperator(
        task_id='extrair_e_popular_tabelas',
        python_callable=extrair_transformar_carregar_sismos
    )