import streamlit as st
import pandas as pd
import sqlite3
import os
import gdown
import plotly.express as px

# URL e nomes de arquivos
URL_DADOS = "https://drive.google.com/uc?export=download&id=1FGFxhoqU75l_9aPo6akxZjN7UNlj1Onb"
ARQUIVO_CSV = "despesa_anual_2024_BRASIL.csv"
ARQUIVO_DB = 'database.db'

def baixar_csv(url, output):
    """Baixa o arquivo CSV do Google Drive."""
    st.info("Iniciando o download do arquivo de dados. Isso pode levar um momento.")
    gdown.download(url, output, quiet=False)
    if os.path.exists(output):
        st.success("Arquivo de dados baixado com sucesso!")
        return True
    st.error(f"Falha ao baixar o arquivo: {output}")
    return False

def processar_dataframe(caminho_csv):
    """Lê e limpa o arquivo CSV, retornando um DataFrame."""
    try:
        df = pd.read_csv(
            caminho_csv, encoding='ISO-8859-1', sep=';', on_bad_lines='skip',
            dtype={'NR_CPF_CNPJ_FORNECEDOR': str, 'CD_TP_DOCUMENTO': 'Int64', 'NM_MUNICIPIO': str},
            na_values=["#NULO#"]
        )
        df['CD_MUNICIPIO'] = df['CD_MUNICIPIO'].replace(-1, None)
        df['VR_PAGAMENTO'] = df['VR_PAGAMENTO'].str.replace(',', '.', regex=False).astype(float)
        df['DT_PAGAMENTO'] = pd.to_datetime(df['DT_PAGAMENTO'].str.strip(), format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        st.error(f"Erro ao processar o arquivo CSV: {e}")
        return None

def criar_tabelas(conn):
    """Cria o schema do banco de dados SQLite."""
    with conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS Local ( CD_MUNICIPIO INTEGER PRIMARY KEY, NM_MUNICIPIO TEXT, SG_UF TEXT );
            CREATE TABLE IF NOT EXISTS Partido ( SG_PARTIDO TEXT PRIMARY KEY, NM_PARTIDO TEXT, DS_TP_ESFERA_PARTIDARIA TEXT );
            CREATE TABLE IF NOT EXISTS Prestador ( NR_CNPJ_PRESTADOR_CONTA TEXT PRIMARY KEY, CD_MUNICIPIO INTEGER, SG_PARTIDO TEXT, FOREIGN KEY (CD_MUNICIPIO) REFERENCES Local(CD_MUNICIPIO), FOREIGN KEY (SG_PARTIDO) REFERENCES Partido(SG_PARTIDO) );
            CREATE TABLE IF NOT EXISTS Fornecedor ( NR_CPF_CNPJ_FORNECEDOR TEXT PRIMARY KEY, NM_FORNECEDOR TEXT, DS_TP_FORNECEDOR TEXT );
            CREATE TABLE IF NOT EXISTS Documento ( NR_CNPJ_PRESTADOR_CONTA TEXT NOT NULL, NR_DOCUMENTO TEXT NOT NULL, CD_TP_DOCUMENTO INTEGER, DS_TP_DOCUMENTO TEXT, PRIMARY KEY (NR_CNPJ_PRESTADOR_CONTA, NR_DOCUMENTO), FOREIGN KEY (NR_CNPJ_PRESTADOR_CONTA) REFERENCES Prestador(NR_CNPJ_PRESTADOR_CONTA) );
            CREATE TABLE IF NOT EXISTS Despesa ( NR_CNPJ_PRESTADOR_CONTA TEXT NOT NULL, NR_CPF_CNPJ_FORNECEDOR TEXT NOT NULL, DT_PAGAMENTO DATE NOT NULL, VR_PAGAMENTO REAL, PRIMARY KEY (NR_CNPJ_PRESTADOR_CONTA, NR_CPF_CNPJ_FORNECEDOR, DT_PAGAMENTO), FOREIGN KEY (NR_CNPJ_PRESTADOR_CONTA) REFERENCES Prestador(NR_CNPJ_PRESTADOR_CONTA), FOREIGN KEY (NR_CPF_CNPJ_FORNECEDOR) REFERENCES Fornecedor(NR_CPF_CNPJ_FORNECEDOR) );
        ''')

def inserir_dados(conn, df):
    """Insere os dados do DataFrame nas tabelas do banco de dados."""
    try:
        with conn:
            df[['CD_MUNICIPIO', 'NM_MUNICIPIO', 'SG_UF']].drop_duplicates('CD_MUNICIPIO').dropna(subset=['CD_MUNICIPIO']).to_sql('Local', conn, if_exists='replace', index=False)
            df[['SG_PARTIDO', 'NM_PARTIDO', 'DS_TP_ESFERA_PARTIDARIA']].drop_duplicates('SG_PARTIDO').dropna(subset=['SG_PARTIDO']).to_sql('Partido', conn, if_exists='replace', index=False)
            df[['NR_CPF_CNPJ_FORNECEDOR', 'NM_FORNECEDOR', 'DS_TP_FORNECEDOR']].drop_duplicates('NR_CPF_CNPJ_FORNECEDOR').dropna(subset=['NR_CPF_CNPJ_FORNECEDOR']).to_sql('Fornecedor', conn, if_exists='replace', index=False)
            df[['NR_CNPJ_PRESTADOR_CONTA', 'CD_MUNICIPIO', 'SG_PARTIDO']].drop_duplicates('NR_CNPJ_PRESTADOR_CONTA').dropna(subset=['NR_CNPJ_PRESTADOR_CONTA']).to_sql('Prestador', conn, if_exists='replace', index=False)
            df[['NR_CNPJ_PRESTADOR_CONTA', 'NR_DOCUMENTO', 'CD_TP_DOCUMENTO', 'DS_TP_DOCUMENTO']].dropna(subset=['NR_DOCUMENTO', 'NR_CNPJ_PRESTADOR_CONTA']).drop_duplicates(['NR_CNPJ_PRESTADOR_CONTA', 'NR_DOCUMENTO']).to_sql('Documento', conn, if_exists='replace', index=False)
            df[['NR_CNPJ_PRESTADOR_CONTA', 'NR_CPF_CNPJ_FORNECEDOR', 'DT_PAGAMENTO', 'VR_PAGAMENTO']].dropna().drop_duplicates(['NR_CNPJ_PRESTADOR_CONTA', 'NR_CPF_CNPJ_FORNECEDOR', 'DT_PAGAMENTO']).to_sql('Despesa', conn, if_exists='replace', index=False)
    except Exception as e:
        st.error(f"Erro ao inserir dados no banco de dados: {e}")

@st.cache_resource
def carregar_dados(url, csv_file, db_file):
    """Função para baixar, processar e carregar os dados, com cache."""
    if os.path.exists(db_file):
         os.remove(db_file) # Garante dados sempre atualizados
            
    if baixar_csv(url, csv_file):
        df = processar_dataframe(csv_file)
        if df is not None:
            conn = sqlite3.connect(db_file, check_same_thread=False)
            criar_tabelas(conn)
            inserir_dados(conn, df)
            return conn
    st.error("Não foi possível carregar os dados. A aplicação não pode continuar.")
    return None

conn = carregar_dados(URL_DADOS, ARQUIVO_CSV, ARQUIVO_DB)

st.set_page_config(layout="wide")

st.title("Análise Interativa de Prestações de Contas Eleitorais - 2024")
st.markdown("Navegue pelas abas abaixo para explorar as despesas políticas sob diversas perspectivas. Use o **Painel de Controle** na barra lateral para aplicar filtros.")

st.sidebar.title("Painel de Controle e Filtros")

if conn is not None:
    tab_titles = [
        "C1: Partidos e Fornecedores", "C2: Prestadores por UF", "C3: Despesa por Tipo Fornecedor", 
        "C4: Prestadores por Partido (BR)", "C5: Top Municípios (Prestadores)", "C6: Gasto Médio por Partido",
        "C7: Contratos por Partido", "C8: Maiores Despesas (Fornecedor)", "C9: Despesas em MG (Jan/24)",
        "C10: Top Municípios (Despesa)", "Explorar Tabelas"
    ]
    tabs = st.tabs(tab_titles)

with tabs[0]:
    st.header("Consulta 1: Partidos com Fornecedores por Faixa de Valor")
    
    st.sidebar.subheader("Filtros da Consulta 1") 
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        valor_minimo_c1 = st.number_input(
            "Valor Mínimo (R$)", 
            min_value=0, 
            max_value=100000, 
            value=10000, 
            step=1000
        )
    with col2:
        valor_maximo_c1 = st.number_input(
            "Valor Máximo (R$)", 
            min_value=0, 
            max_value=100000, 
            value=50000, 
            step=1000
        )

    query1 = """
    SELECT 
        f.NM_FORNECEDOR, 
        dp.VR_PAGAMENTO 
    FROM 
        Partido p 
        NATURAL JOIN Prestador pr 
        NATURAL JOIN Despesa dp 
        NATURAL JOIN Fornecedor f 
    WHERE 
        p.DS_TP_ESFERA_PARTIDARIA = 'Nacional' 
        AND dp.VR_PAGAMENTO BETWEEN ? AND ?
    """
    
    df1 = pd.read_sql_query(query1, conn, params=[valor_minimo_c1, valor_maximo_c1])

    if not df1.empty:
        df_grafico1 = df1.groupby("NM_FORNECEDOR")["VR_PAGAMENTO"].sum().nlargest(10).reset_index()
        
        fig1 = px.bar(
            df_grafico1, 
            y="NM_FORNECEDOR", 
            x="VR_PAGAMENTO",
            orientation='h', 
            title=f"Top 10 Fornecedores com Pagamentos entre R${valor_minimo_c1:,.2f} e R${valor_maximo_c1:,.2f}",
            labels={'VR_PAGAMENTO': 'Montante Total (R$)', 'NM_FORNECEDOR': 'Fornecedor'},
            text_auto='.2s'
        )
        fig1.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig1, use_container_width=True)
        
        with st.expander("Visualizar dados tabulares da consulta"): 
            st.dataframe(df1)
    else: 
        st.warning("Nenhum dado encontrado para a faixa de valores selecionada.")

    with tabs[1]:

        st.header("Consulta 2: Análise de Prestadores por Partido e UF")

        st.sidebar.subheader("Filtros da Consulta 2") 

        df2_base = pd.read_sql_query("SELECT P.NM_PARTIDO, L.SG_UF FROM Prestador Pr NATURAL JOIN Partido P NATURAL JOIN Local L", conn)
        
        if not df2_base.empty:
            estados_disponiveis = sorted(df2_base['SG_UF'].unique())
            estado_selecionado = st.sidebar.selectbox("C2: Selecione o Estado", estados_disponiveis, index=estados_disponiveis.index('SP'))
            df2_filtrado = df2_base[df2_base['SG_UF'] == estado_selecionado]
            
            if not df2_filtrado.empty:
                df_grafico2 = df2_filtrado.groupby("NM_PARTIDO").size().reset_index(name='Numero_Prestadores').sort_values('Numero_Prestadores', ascending=False)
                fig2 = px.bar(df_grafico2, x='NM_PARTIDO', y='Numero_Prestadores', title=f"Número de Prestadores por Partido em {estado_selecionado}", labels={'NM_PARTIDO': 'Partido', 'Numero_Prestadores': 'Número de Prestadores'})
                st.plotly_chart(fig2, use_container_width=True)
                with st.expander("Visualizar dados tabulares da consulta"): st.dataframe(df2_filtrado)
            else: st.warning(f"Nenhum dado encontrado para o estado {estado_selecionado}.")
        else: st.warning("Não foi possível carregar os dados para esta consulta.")



with tabs[2]:
    st.header("Consulta 3: Análise de Despesas por Tipo de Fornecedor")

    st.sidebar.subheader("Filtros da Consulta 3")
    
    tipo_fornecedor = st.sidebar.radio(
        "Selecione o Tipo de Fornecedor para detalhar:",
        options=['PESSOA JURÍDICA', 'PESSOA FÍSICA'],
        index=0  
    )

    
    st.subheader("Visão Geral: Distribuição de Pagamentos")
    df3_geral = pd.read_sql_query("SELECT F.DS_TP_FORNECEDOR, SUM(D.VR_PAGAMENTO) AS VR_TOTAL FROM Fornecedor F NATURAL JOIN Despesa D GROUP BY F.DS_TP_FORNECEDOR", conn)
    if not df3_geral.empty:
        fig3_pie = px.pie(df3_geral, names='DS_TP_FORNECEDOR', values='VR_TOTAL', title="Distribuição de Pagamentos por Tipo de Fornecedor", hole=.3)
        st.plotly_chart(fig3_pie, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado para a visão geral.")

    
    st.subheader(f"Top 10 Fornecedores do Tipo: {tipo_fornecedor}")
    
    
    query3_detalhe = """
        SELECT 
            F.NM_FORNECEDOR, 
            SUM(D.VR_PAGAMENTO) as Total_Recebido
        FROM 
            Fornecedor F 
            NATURAL JOIN Despesa D
        WHERE 
            F.DS_TP_FORNECEDOR = ?
        GROUP BY 
            F.NM_FORNECEDOR
        ORDER BY 
            Total_Recebido DESC
        LIMIT 10
    """
    df3_detalhe = pd.read_sql_query(query3_detalhe, conn, params=[tipo_fornecedor])

    if not df3_detalhe.empty:
        fig3_bar = px.bar(
            df3_detalhe,
            x='Total_Recebido',
            y='NM_FORNECEDOR',
            orientation='h',
            title=f"Top 10 Fornecedores ({tipo_fornecedor}) por Valor Recebido",
            labels={'NM_FORNECEDOR': 'Fornecedor', 'Total_Recebido': 'Total Recebido (R$)'},
            text_auto='.2s'
        )
        fig3_bar.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig3_bar, use_container_width=True)

        with st.expander("Visualizar dados detalhados da consulta"):
            st.dataframe(df3_detalhe)
    else:
        st.warning(f"Nenhum fornecedor do tipo '{tipo_fornecedor}' encontrado nos dados.")


with tabs[3]:
    st.header("Consulta 4: Número de Prestadores por Partido")

    st.markdown("Análise do número de prestadores de contas por partido, com filtro geográfico.")

    st.sidebar.subheader("Filtros da Consulta 4")
    
    df_estados = pd.read_sql_query("SELECT DISTINCT SG_UF FROM Local ORDER BY SG_UF", conn)
    lista_estados = df_estados['SG_UF'].tolist()

    estados_selecionados = st.sidebar.multiselect(
        "Selecione um ou mais Estados:",
        options=lista_estados,
        default=lista_estados  
    )

    if not estados_selecionados:
        st.warning("Por favor, selecione ao menos um estado no painel de filtros.")
    else:
        
        placeholders = ', '.join('?' for _ in estados_selecionados)
        
        query4 = f"""
            SELECT 
                P.SG_PARTIDO, 
                COUNT(Pr.NR_CNPJ_PRESTADOR_CONTA) AS Numero_Prestadores 
            FROM 
                Prestador Pr 
                NATURAL JOIN Partido P 
                NATURAL JOIN Local L
            WHERE 
                L.SG_UF IN ({placeholders})
            GROUP BY 
                P.SG_PARTIDO 
            ORDER BY 
                Numero_Prestadores DESC
        """
        
        df4 = pd.read_sql_query(query4, conn, params=estados_selecionados)

        if not df4.empty:
           
            if len(estados_selecionados) == len(lista_estados):
                titulo_grafico = "Número de Prestadores por Partido (Brasil)"
            elif len(estados_selecionados) > 1:
                titulo_grafico = f"Número de Prestadores por Partido ({len(estados_selecionados)} estados selecionados)"
            else:
                titulo_grafico = f"Número de Prestadores por Partido ({estados_selecionados[0]})"

            fig4 = px.bar(
                df4, 
                x='SG_PARTIDO', 
                y='Numero_Prestadores', 
                title=titulo_grafico, 
                labels={'SG_PARTIDO': 'Partido', 'Numero_Prestadores': 'Número de Prestadores'}
            )
            st.plotly_chart(fig4, use_container_width=True)

            with st.expander("Visualizar dados tabulares da consulta"):
                st.dataframe(df4)
        else:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")

    with tabs[4]:

        st.header("Consulta 5: Municípios com Maior Número de Prestadores")
        
        st.sidebar.subheader("Filtros da Consulta 5")

        top_n_c5 = st.sidebar.slider("Quantos municípios exibir?", 5, 50, 10)

        df5 = pd.read_sql_query(f"SELECT L.NM_MUNICIPIO, COUNT(Pr.NR_CNPJ_PRESTADOR_CONTA) AS n FROM Prestador Pr NATURAL JOIN Local L GROUP BY L.NM_MUNICIPIO ORDER BY n DESC LIMIT {top_n_c5}", conn)
        if not df5.empty:
            fig5 = px.bar(df5, x='n', y='NM_MUNICIPIO', orientation='h', title=f'Top {top_n_c5} Municípios com Mais Prestadores', labels={'NM_MUNICIPIO': 'Município', 'n': 'Número de Prestadores'})
            fig5.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig5, use_container_width=True)
            with st.expander("Visualizar dados tabulares"): st.dataframe(df5)
        else: st.warning("Nenhum dado encontrado.")

with tabs[5]:
    st.header("Consulta 6: Valor Médio de Pagamento por Partido")

    st.markdown("Análise da média de gastos por partido, com filtros geográficos e temporais.")

    st.sidebar.subheader("Filtros da Consulta 6")

    df_estados_c6 = pd.read_sql_query("SELECT DISTINCT SG_UF FROM Local ORDER BY SG_UF", conn)
    lista_estados_c6 = df_estados_c6['SG_UF'].tolist()
    
    datas_disponiveis = pd.read_sql_query("SELECT MIN(DT_PAGAMENTO) as min_date, MAX(DT_PAGAMENTO) as max_date FROM Despesa", conn)
    min_data = pd.to_datetime(datas_disponiveis['min_date'].iloc[0])
    max_data = pd.to_datetime(datas_disponiveis['max_date'].iloc[0])

    estados_selecionados_c6 = st.sidebar.multiselect(
        "Selecione os Estados:",
        options=lista_estados_c6,
        default=lista_estados_c6  
    )

    data_selecionada_c6 = st.sidebar.date_input(
        "Selecione o Período:",
        value=(min_data, max_data),
        min_value=min_data,
        max_value=max_data,
        format="DD/MM/YYYY"
    )

    if not estados_selecionados_c6:
        st.warning("Por favor, selecione ao menos um estado no painel de filtros.")
    elif len(data_selecionada_c6) != 2:
        st.warning("Por favor, selecione um intervalo de datas válido (início e fim).")
    else:
        data_inicio, data_fim = data_selecionada_c6
        
        placeholders_estados = ', '.join('?' for _ in estados_selecionados_c6)
        query6 = f"""
            SELECT 
                PT.SG_PARTIDO, 
                AVG(D.VR_PAGAMENTO) AS Media_Gastos 
            FROM 
                Despesa D 
                NATURAL JOIN Prestador P 
                NATURAL JOIN Partido PT 
                NATURAL JOIN Local L
            WHERE 
                L.SG_UF IN ({placeholders_estados}) 
                AND D.DT_PAGAMENTO BETWEEN ? AND ?
            GROUP BY 
                PT.SG_PARTIDO 
            ORDER BY 
                Media_Gastos DESC
        """
        
        params = estados_selecionados_c6 + [data_inicio.strftime('%Y-%m-%d'), data_fim.strftime('%Y-%m-%d')]
        df6 = pd.read_sql_query(query6, conn, params=params)

        if not df6.empty:
            titulo_grafico = f"Média de Gastos por Partido ({len(estados_selecionados_c6)} Estados)"
            
            fig6 = px.bar(
                df6, 
                x='SG_PARTIDO', 
                y='Media_Gastos', 
                title=titulo_grafico, 
                labels={'SG_PARTIDO': 'Partido', 'Media_Gastos': 'Média de Gastos (R$)'}, 
                text_auto='.2s'
            )
            st.plotly_chart(fig6, use_container_width=True)

            with st.expander("Visualizar dados tabulares da consulta"):
                st.dataframe(df6)
        else:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")


    with tabs[6]:

        st.header("Consulta 7: Quantidade de Contratos Firmados por Partido")

        st.sidebar.subheader("Filtros da Consulta 7")

        top_n_c7 = st.sidebar.slider("Quantos partidos exibir?", 5, 30, 10)
        df7 = pd.read_sql_query(f"SELECT P.SG_PARTIDO, COUNT(D.NR_CPF_CNPJ_FORNECEDOR) AS n FROM Despesa D NATURAL JOIN Prestador P GROUP BY P.SG_PARTIDO ORDER BY n DESC LIMIT {top_n_c7}", conn)
        if not df7.empty:
            fig7 = px.bar(df7, x='n', y='SG_PARTIDO', orientation='h', title=f"Top {top_n_c7} Partidos com Mais Contratos", labels={'SG_PARTIDO': 'Partido', 'n': 'Quantidade de Contratos'})
            fig7.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig7, use_container_width=True)
            with st.expander("Visualizar dados tabulares"): st.dataframe(df7)
        else: st.warning("Nenhum dado encontrado.")

    with tabs[7]:

        st.header("Consulta 8: Fornecedores com Maior Volume de Despesas em 2024")

        st.sidebar.subheader("Filtros da Consulta 8")

        top_n_c8 = st.sidebar.slider("Quantos fornecedores exibir?", 5, 50, 10)
        df8 = pd.read_sql_query(f"SELECT F.NM_FORNECEDOR, SUM(D.VR_PAGAMENTO) AS Total FROM Despesa D NATURAL JOIN Fornecedor F WHERE D.DT_PAGAMENTO BETWEEN '2024-01-01' AND '2024-12-31' GROUP BY F.NM_FORNECEDOR ORDER BY Total DESC LIMIT {top_n_c8}", conn)
        if not df8.empty:
            fig8 = px.bar(df8, x='Total', y='NM_FORNECEDOR', orientation='h', title=f"Top {top_n_c8} Maiores Despesas por Fornecedor (2024)", labels={'NM_FORNECEDOR': 'Fornecedor', 'Total': 'Despesa Total (R$)'}, text_auto='.2s')
            fig8.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig8, use_container_width=True)
            with st.expander("Visualizar dados tabulares"): st.dataframe(df8)
        else: st.warning("Nenhum dado encontrado.")


with tabs[8]:

    st.header("Consulta 9: Total de Despesas por Partido")
    st.markdown("Análise de despesas partidárias, com filtros por estado e período.")

    st.sidebar.subheader("Filtros da Consulta 9")

    df_estados_c9 = pd.read_sql_query("SELECT DISTINCT SG_UF FROM Local ORDER BY SG_UF", conn)
    lista_estados_c9 = df_estados_c9['SG_UF'].tolist()

    default_ix_c9 = lista_estados_c9.index('MG') if 'MG' in lista_estados_c9 else 0
    estado_selecionado_c9 = st.sidebar.selectbox(
        "Selecione o Estado:",
        options=lista_estados_c9,
        index=default_ix_c9
    )

    data_selecionada_c9 = st.sidebar.date_input(
        "Selecione o Período:",
        value=(pd.to_datetime("2024-01-01"), pd.to_datetime("2024-01-31")),
        format="DD/MM/YYYY"
    )

    if len(data_selecionada_c9) != 2:
        st.warning("Por favor, selecione um intervalo de datas válido (início e fim).")
    else:
        data_inicio_c9, data_fim_c9 = data_selecionada_c9
        
        query9 = """
            SELECT 
                Pt.NM_PARTIDO, 
                SUM(D.VR_PAGAMENTO) AS Total_Despesas
            FROM 
                Despesa D 
                NATURAL JOIN Prestador P 
                NATURAL JOIN Local L 
                NATURAL JOIN Partido Pt
            WHERE 
                L.SG_UF = ? 
                AND D.DT_PAGAMENTO BETWEEN ? AND ?
            GROUP BY 
                Pt.NM_PARTIDO
            ORDER BY 
                Total_Despesas DESC
        """
        
        params_c9 = [estado_selecionado_c9, data_inicio_c9.strftime('%Y-%m-%d'), data_fim_c9.strftime('%Y-%m-%d')]
        df9 = pd.read_sql_query(query9, conn, params=params_c9)

        if not df9.empty:

            titulo_grafico = f"Total de Despesas em {estado_selecionado_c9} ({data_inicio_c9.strftime('%d/%m/%Y')} a {data_fim_c9.strftime('%d/%m/%Y')})"
            
            fig9 = px.bar(
                df9, 
                x='Total_Despesas', 
                y='NM_PARTIDO', 
                orientation='h', 
                title=titulo_grafico, 
                labels={'NM_PARTIDO': 'Partido', 'Total_Despesas': 'Total de Despesas (R$)'}
            )
            fig9.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig9, use_container_width=True)

            with st.expander("Visualizar dados tabulares da consulta"):
                st.dataframe(df9)
        else:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")

    with tabs[9]:

        st.header("Consulta 10: Municípios com Maior Total de Despesas em 2024")

        st.sidebar.subheader("Filtros da Consulta 10")

        top_n_c10 = st.sidebar.slider("Quantos municípios exibir?", 3, 15, 5)
        df10 = pd.read_sql_query(f"SELECT L.NM_MUNICIPIO, SUM(D.VR_PAGAMENTO) AS Total FROM Despesa D NATURAL JOIN Prestador P NATURAL JOIN Local L WHERE D.DT_PAGAMENTO BETWEEN '2024-01-01' AND '2024-12-31' GROUP BY L.NM_MUNICIPIO ORDER BY Total DESC LIMIT {top_n_c10}", conn)
        if not df10.empty:
            fig10 = px.pie(df10, values='Total', names='NM_MUNICIPIO', title=f'Distribuição de Despesas entre os Top {top_n_c10} Municípios (2024)')
            st.plotly_chart(fig10, use_container_width=True)
            with st.expander("Visualizar dados tabulares"): st.dataframe(df10)
        else: st.warning("Nenhum dado encontrado.")

    # --- Aba 11: Explorador de Tabelas ---
    with tabs[10]:
        st.header("Explorador de Tabelas do Banco de Dados")
        tabela_selecionada = st.selectbox("Selecione uma tabela para explorar", options=['Local', 'Partido', 'Fornecedor', 'Prestador', 'Documento', 'Despesa'])
        if tabela_selecionada:
            df_tabela = pd.read_sql_query(f"SELECT * FROM {tabela_selecionada}", conn)
            st.dataframe(df_tabela)
            st.info(f"Total de registros na tabela `{tabela_selecionada}`: {len(df_tabela)}")
        else:
            st.error("A conexão com o banco de dados falhou. A aplicação não pode ser exibida.")