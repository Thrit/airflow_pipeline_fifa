import re
import pandas as pd

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def transform_data():

    df_fifa = pd.read_csv(f'/opt/airflow/raw_data/fifa21 raw data v2.csv', sep=',', dtype=str)

    for col in df_fifa.columns:
        df_fifa.rename(columns={col:col.lower()
                                .replace(' ', '_')
                                .replace('/', '_')
                                .replace('↓', '')}, inplace=True)

    df_fifa['age'] = df_fifa['age'].astype('int64')

    def type_contract_normalization(value: str) -> list:
        """
        Check the type of contract and return a list of split information

        Parameters:
        ----------

            value: Row with the value to be checked
        """

        pattern = r'(Jan(uary)?|Feb(ruary)?|Mar(ch)?|Apr(il)?|May|Jun(e)?|Jul(y)?|Aug(ust)?|\
        Sep(tember)?|Oct(ober)?|Nov(ember)?|Dec(ember)?)\s+\d{1,2},\s+\d{4}'

        if 'Loan' in value:
            return ['on loan', re.search(pattern, value).group(0), pd.NaT, pd.NaT]
        elif 'Free' in value:
            return ['free', pd.NaT, pd.NaT, pd.NaT]
        else:
            return ['hired', pd.NaT, value.split(' ~ ')[0], value.split(' ~ ')[1]]

    aggr = ['contract_type', 'loan_date_start', 'contract_year_start', 'contract_year_end']
    df_fifa = df_fifa.join(df_fifa['contract'].apply(
        lambda x: pd.Series(type_contract_normalization(x), index=aggr)
    ))

    df_fifa['loan_date_start'] = pd.to_datetime(df_fifa['loan_date_start'])

    dict_feet_to_meters = {
        '4\'0"': '122',
        '4\'1"': '124',
        '4\'2"': '127',
        '4\'3"': '130',
        '4\'4"': '132',
        '4\'5"': '135',
        '4\'6"': '137',
        '4\'7"': '140',
        '4\'8"': '142',
        '4\'9"': '145',
        '4\'10"': '147',
        '4\'11"': '150',
        '5\'0"': '152',
        '5\'1"': '155',
        '5\'2"': '157',
        '5\'3"': '160',
        '5\'4"': '163',
        '5\'5"': '165',
        '5\'6"': '168',
        '5\'7"': '170',
        '5\'8"': '173',
        '5\'9"': '175',
        '5\'10"': '178',
        '5\'11"': '180',
        '6\'0"': '183',
        '6\'1"': '185',
        '6\'2"': '188',
        '6\'3"': '191',
        '6\'4"': '193',
        '6\'5"': '196',
        '6\'6"': '198',
        '6\'7"': '201',
        '6\'8"': '203',
        '6\'9"': '206',
        '6\'10"': '208',
        '6\'11"': '211',
    }

    df_fifa['height'] = df_fifa['height'].str.replace('cm', '')
    df_fifa['height'] = df_fifa['height'].apply(lambda x: dict_feet_to_meters.get(x, x))
    df_fifa['height'] = df_fifa['height'].astype('int64')

    def lbs_to_kg(value: str) -> int:
        value = int(value.replace('lbs', ''))
        return value * 0.45359237

    df_fifa['weight'] = df_fifa['weight'].str.replace('kg', '')
    df_fifa['weight'] = df_fifa['weight'].apply(
        lambda x: lbs_to_kg(x) if 'lbs' in x else x
    )
    df_fifa['weight'] = df_fifa['weight'].astype('int64')

    def fix_zeros(value: str, symbol: str, zeros_count: int) -> int:
        """
        Replace the K and M with their respectives thousand separator

        Parameters:
        ----------

            value: Row with the value to be converted
            symbol: M: Million or K: Thousands
            zeros_count: Quantity of zeros to convert according to its symbol
        """

        copy = value.replace(symbol, '')
        value = value.replace('.', '')

        if symbol == 'K':
            return value.replace(symbol, '0' * zeros_count)
        else:
            return value.replace(symbol, '0' * zeros_count)

    normalize_money = ['value', 'wage', 'release_clause']
    for col in normalize_money:
        df_fifa[col] = df_fifa[col].str.replace('€', '')

        k_mask = df_fifa[col].str.contains('K', na=False)
        m_mask = df_fifa[col].str.contains('M', na=False)

        k_values_copy = df_fifa.loc[k_mask, col].copy()
        m_values_copy = df_fifa.loc[m_mask, col].copy()

        df_fifa.loc[k_mask, col] = k_values_copy.map(lambda x: fix_zeros(str(x).strip(), 'K', 3))
        df_fifa.loc[m_mask, col] = m_values_copy.map(lambda x: fix_zeros(str(x).strip(), 'M', 6))

        df_fifa[col] = df_fifa[col].astype('int64')

    remove_stars = ['w_f', 'sm', 'ir']
    for col in remove_stars:
        df_fifa[col] = df_fifa[col].str.replace(' ', '').str.replace('★', '')
        df_fifa[col] = df_fifa[col].astype('int64')

    df_fifa['hits'] = df_fifa['hits'].str.replace('€', '')

    k_mask = df_fifa['hits'].str.contains('K', na=False)
    m_mask = df_fifa['hits'].str.contains('M', na=False)

    k_values_copy = df_fifa.loc[k_mask, 'hits'].copy()
    m_values_copy = df_fifa.loc[m_mask, 'hits'].copy()

    df_fifa.loc[k_mask, 'hits'] = k_values_copy.map(lambda x: fix_zeros(str(x).strip(), 'K', 3))
    df_fifa.loc[m_mask, 'hits'] = m_values_copy.map(lambda x: fix_zeros(str(x).strip(), 'M', 6))

    df_fifa['hits'] = df_fifa['hits'].astype(float).astype('Int64')

    column_to_int = [
        'ova',
        'pot',
        'bov',
        'attacking',
        'crossing',
        'finishing',
        'heading_accuracy',
        'short_passing',
        'volleys',
        'skill',
        'dribbling',
        'curve',
        'fk_accuracy',
        'long_passing',
        'ball_control',
        'movement',
        'acceleration',
        'sprint_speed',
        'agility',
        'reactions',
        'balance',
        'power',
        'shot_power',
        'jumping',
        'stamina',
        'strength',
        'long_shots',
        'mentality',
        'aggression',
        'interceptions',
        'positioning',
        'vision',
        'penalties',
        'composure',
        'defending',
        'marking',
        'standing_tackle',
        'sliding_tackle',
        'goalkeeping',
        'gk_diving',
        'gk_handling',
        'gk_kicking',
        'gk_positioning',
        'gk_reflexes',
        'total_stats',
        'base_stats',
        'pac',
        'sho',
        'pas',
        'dri',
        'def',
        'phy',
    ]
    for col in column_to_int:
        df_fifa[col] = df_fifa[col].astype('int64')

    df_fifa['club'] = df_fifa['club'].str.replace('\n', '')
    df_fifa['joined'] = pd.to_datetime(df_fifa['joined'])
    df_fifa['loan_date_end'] = pd.to_datetime(df_fifa['loan_date_end'])

    categorical_columns = ['nationality', 'club', 'preferred_foot', 'best_position', 'contract_type']
    for col in categorical_columns:
        df_fifa[col] = df_fifa[col].astype('category')

    categorical_columns = ['a_w', 'd_w']
    for col in categorical_columns:
        df_fifa[col] = df_fifa[col].astype('category')

    df_fifa.to_csv(f'/opt/airflow/processed_data/processed_data.csv', index=False)

    print(df_fifa.columns)


def load_data() -> None:

    from sqlalchemy import create_engine

    df = pd.read_csv(f'/opt/airflow/processed_data/processed_data.csv')

    engine_url = f'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    engine = create_engine(engine_url)
    df.to_sql('fifa_database', engine, if_exists='replace')


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

ingestion_dag = DAG(
    'fifa_ingestion',
    default_args=default_args,
    description='Ingest the FIFA database. Collected in Kaggle',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag
)

task_1 >> task_2
