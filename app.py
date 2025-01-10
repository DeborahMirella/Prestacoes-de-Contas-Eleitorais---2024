import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
import os
import gdown

# URL e arquivos
url = "https://drive.google.com/uc?export=download&id=1FGFxhoqU75l_9aPo6akxZjN7UNlj1Onb"
output = "despesa_anual_2024_BRASIL.csv"
db_file = 'database.db'
sql_file = 'database.sql'

# Função para baixar arquivo CSV
def baixar_csv(url, output):
    gdown.download(url, output, quiet=False)
    if os.path.exists(output):
        print("Arquivo CSV baixado com sucesso!")
        return True
    else:
        print(f"Falha ao baixar o arquivo: {output}")
        return False

# Função para processar o DataFrame
def processar_dataframe(caminho_csv):
    try:
        df = pd.read_csv(
            caminho_csv,
            encoding='ISO-8859-1',
            sep=';', # Define o separador como ponto-e-vírgula
            on_bad_lines='skip', # Ignora linhas com erros de formatação
            dtype={
                'NR_CPF_CNPJ_FORNECEDOR': str,
                'CD_TP_DOCUMENTO': 'Int64',
                'NM_MUNICIPIO': str,
            },
            na_values=["#NULO#"] # '#NULO#' será substituído por NaN (valor ausente)
        )

        # Substitui valores '-1' na coluna 'CD_MUNICIPIO' por None
        df['CD_MUNICIPIO'] = df['CD_MUNICIPIO'].replace(-1, None)

        # Substitui vírgulas por pontos na coluna 'VR_PAGAMENTO' para garantir a correta interpretação caso haja valores com casas decimais
        df['VR_PAGAMENTO'] = df['VR_PAGAMENTO'].str.replace(',', '.', regex=False)

        # Converte o formato das datas para 'YYYY-MM-DD' (compatível com SQLite)
        df['DT_PAGAMENTO'] = pd.to_datetime(
            df['DT_PAGAMENTO'].str.strip(),
            format='%d/%m/%Y',
            errors='coerce' # Substitui valores inválidos por NaT (Not a Time)
        ).dt.strftime('%Y-%m-%d')

        if df.empty:
            print("O arquivo CSV está vazio.")
            return None
        print("Arquivo CSV processado com sucesso!")
        return df
    except Exception as e:
        print(f"Erro ao processar o arquivo CSV: {e}")
        return None

# Função para criar tabelas SQLite
def criar_tabelas(conn):
    with conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript('''
            CREATE TABLE Local (
                CD_MUNICIPIO INTEGER PRIMARY KEY,
                NM_MUNICIPIO TEXT,
                SG_UF TEXT
            );

            CREATE TABLE Partido (
                SG_PARTIDO TEXT PRIMARY KEY,
                NM_PARTIDO TEXT,
                DS_TP_ESFERA_PARTIDARIA TEXT
            );

            CREATE TABLE Prestador (
                NR_CNPJ_PRESTADOR_CONTA TEXT PRIMARY KEY,
                CD_MUNICIPIO INTEGER,
                SG_PARTIDO TEXT,
                FOREIGN KEY (CD_MUNICIPIO) REFERENCES Local(CD_MUNICIPIO),
                FOREIGN KEY (SG_PARTIDO) REFERENCES Partido(SG_PARTIDO)
            );

            CREATE TABLE Fornecedor (
                NR_CPF_CNPJ_FORNECEDOR TEXT PRIMARY KEY,
                NM_FORNECEDOR TEXT,
                DS_TP_FORNECEDOR TEXT
            );

            CREATE TABLE Documento (
                NR_CNPJ_PRESTADOR_CONTA TEXT NOT NULL,
                NR_DOCUMENTO TEXT NOT NULL,
                CD_TP_DOCUMENTO INTEGER,
                DS_TP_DOCUMENTO TEXT,
                PRIMARY KEY (NR_CNPJ_PRESTADOR_CONTA, NR_DOCUMENTO),
                FOREIGN KEY (NR_CNPJ_PRESTADOR_CONTA) REFERENCES Prestador(NR_CNPJ_PRESTADOR_CONTA)
            );

            CREATE TABLE Despesa (
                NR_CNPJ_PRESTADOR_CONTA TEXT NOT NULL,
                NR_CPF_CNPJ_FORNECEDOR TEXT NOT NULL,
                DT_PAGAMENTO DATE NOT NULL,
                VR_PAGAMENTO REAL,
                PRIMARY KEY (NR_CNPJ_PRESTADOR_CONTA, NR_CPF_CNPJ_FORNECEDOR, DT_PAGAMENTO),
                FOREIGN KEY (NR_CNPJ_PRESTADOR_CONTA) REFERENCES Prestador(NR_CNPJ_PRESTADOR_CONTA),
                FOREIGN KEY (NR_CPF_CNPJ_FORNECEDOR) REFERENCES Fornecedor(NR_CPF_CNPJ_FORNECEDOR)
            );
        ''')
        print("Tabelas criadas com sucesso!")

# Função para inserir dados nas tabelas
def inserir_dados(conn, df):
    try:
        with conn:
            # Inserir dados na tabela Local
            Local = (
                df[['CD_MUNICIPIO', 'NM_MUNICIPIO', 'SG_UF']]
                .drop_duplicates(subset=['CD_MUNICIPIO'])
                .dropna(subset=['CD_MUNICIPIO']) # Remove registros com valores ausentes em 'CD_MUNICIPIO' (chave primária) e redefine os índices
                .reset_index(drop=True) # Redefine os índices
            )
            Local.to_sql('Local', conn, if_exists='append', index=False)

            # Inserir dados na tabela Partido
            Partido = (
              df[['SG_PARTIDO', 'NM_PARTIDO', 'DS_TP_ESFERA_PARTIDARIA']]
              .drop_duplicates(subset=['SG_PARTIDO'])
              .dropna(subset=['SG_PARTIDO'])
              .reset_index(drop=True)
            )
            Partido.to_sql('Partido', conn, if_exists='append', index=False)

            # Inserir dados na tabela Fornecedor
            Fornecedor = (
              df[['NR_CPF_CNPJ_FORNECEDOR', 'NM_FORNECEDOR', 'DS_TP_FORNECEDOR']]
              .drop_duplicates(subset=['NR_CPF_CNPJ_FORNECEDOR'])
              .dropna(subset=['NR_CPF_CNPJ_FORNECEDOR'])
              .reset_index(drop=True)
            )
            Fornecedor.to_sql('Fornecedor', conn, if_exists='append', index=False)

            # Inserir dados na tabela Prestador
            Prestador = (
                df[['NR_CNPJ_PRESTADOR_CONTA', 'CD_MUNICIPIO', 'SG_PARTIDO']]
                .drop_duplicates(subset=['NR_CNPJ_PRESTADOR_CONTA'])
                .dropna(subset=['NR_CNPJ_PRESTADOR_CONTA'])
                .reset_index(drop=True)
            )
            Prestador.to_sql('Prestador', conn, if_exists='append', index=False)

            # Inserir dados na tabela Documento
            Documento = (
              df[['NR_CNPJ_PRESTADOR_CONTA', 'NR_DOCUMENTO', 'CD_TP_DOCUMENTO', 'DS_TP_DOCUMENTO']]
              .dropna(subset=['NR_DOCUMENTO'])
              .dropna(subset=['NR_CNPJ_PRESTADOR_CONTA'])
              .drop_duplicates(subset=['NR_CNPJ_PRESTADOR_CONTA', 'NR_DOCUMENTO'])
              .reset_index(drop=True)
            )
            Documento.to_sql('Documento', conn, if_exists='append', index=False)

            # Inserir dados na tabela Despesa
            Despesa = (
              df[['NR_CNPJ_PRESTADOR_CONTA', 'NR_CPF_CNPJ_FORNECEDOR', 'DT_PAGAMENTO', 'VR_PAGAMENTO']]
                .dropna(subset=['NR_CNPJ_PRESTADOR_CONTA'])
                .dropna(subset=['NR_CPF_CNPJ_FORNECEDOR'])
                .dropna(subset=['DT_PAGAMENTO'])
                .drop_duplicates(subset=['NR_CNPJ_PRESTADOR_CONTA', 'NR_CPF_CNPJ_FORNECEDOR', 'DT_PAGAMENTO'])
                .reset_index(drop=True)
            )
            Despesa.to_sql('Despesa', conn, if_exists='append', index=False)

            print("Dados inseridos com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")

# Função para exportar o banco de dados para SQL
def exportar_sql(conn, arquivo_sql):
    # Criação de um dicionário com o nome das tabelas e seus dados no dataframe
    tabelas = {
        "Local": pd.read_sql_query("SELECT * FROM Local", conn),
        "Partido": pd.read_sql_query("SELECT * FROM Partido", conn),
        "Fornecedor": pd.read_sql_query("SELECT * FROM Fornecedor", conn),
        "Prestador": pd.read_sql_query("SELECT * FROM Prestador", conn),
        "Documento": pd.read_sql_query("SELECT * FROM Documento", conn),
        "Despesa": pd.read_sql_query("SELECT * FROM Despesa", conn),
    }

    with open('database.sql', 'w', encoding='utf-8') as f:
        # Iterar sobre cada tabela para gerar seu script SQL
        for nome_tabela, df in tabelas.items():
            f.write(f"-- Criação da tabela {nome_tabela}\n")
            f.write(f"DROP TABLE IF EXISTS {nome_tabela};\n") # Comando para excluir a tabela caso já exista
            f.write(pd.io.sql.get_schema(df, nome_tabela, con=conn)) # Obter o esquema SQL para a criação da tabela e escrever no arquivo

            f.write(";\n\n")
            f.write(f"-- Dados da tabela {nome_tabela}\n")

            # Iterar sobre as linhas da tabela para gerar os comandos INSERT
            for _, row in df.iterrows():
                valores = []
                for x in row:
                    if pd.isna(x):  # Se o valor for NaN, substituir por NULL
                        valores.append("NULL")
                    elif isinstance(x, str):  # Se for string, adicionar aspas simples
                        valores.append("'" + x.replace("'", "''") + "'")
                    else:  # Caso contrário, usar o valor diretamente
                        valores.append(str(x))
                valores_str = ", ".join(valores)
                f.write(f"INSERT INTO {nome_tabela} VALUES ({valores_str});\n") # Escrever o comando INSERT para a linha atual
            f.write("\n\n")
    print("Arquivo SQL gerado com sucesso.")

# Workflow principal
if baixar_csv(url, output):
    st.write("Arquivo CSV baixado com sucesso.")
    df = processar_dataframe(output)
    if df is not None:
        if os.path.exists(db_file):
            os.remove(db_file)
        conn = sqlite3.connect(db_file)
        criar_tabelas(conn)
        inserir_dados(conn, df)

# Título geral
st.title("Prestações de contas eleitorais - 2024")

st.text("As consultas apresentadas têm como objetivo analisar e interpretar dados relacionados a despesas e contratos políticos no Brasil, utilizando diferentes perspectivas, como partidos, fornecedores, prestadores de contas, estados e municípios")

#Barra Lateral

# Função para explorar tabelas genéricas
def explorar_tabela(conn, tabela, colunas_formatar=None, formato="{:.2f}"):
    st.subheader(f"Explorando a Tabela: {tabela}")
    try:
        # Obter os dados da tabela
        query = f"SELECT * FROM {tabela}"
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            st.write(f"A tabela '{tabela}' está vazia.")
            return
        
        # Slider para limitar número de linhas exibidas
        qntd_linhas = st.sidebar.slider(
            f"Quantas linhas deseja visualizar na tabela '{tabela}'?",
            min_value=1,
            max_value=min(len(df), 500),  # Limitar para até 500 registros por vez
            value=10
        )
        
        # Exibir a tabela formatada
        if colunas_formatar:
            st.write(df.head(qntd_linhas).style.format(subset=colunas_formatar, formatter=formato))
        else:
            st.write(df.head(qntd_linhas))
        
        # Mostrar número total de registros
        st.write(f"Número total de registros: {len(df)}")
    except Exception as e:
        st.error(f"Erro ao carregar a tabela '{tabela}': {e}")

# Menu da barra lateral
st.sidebar.title("Menu de Navegação")
opcao = st.sidebar.radio(
    "Escolha o que deseja visualizar:",
    ("Consulta 1", "Consulta 2", "Consulta 3", "Consulta 4", "Consulta 5", "Consulta 6", "Consulta 7", "Consulta 8", "Consulta 9", "Consulta 10", "Explorar Tabelas")
)

# Consulta 1: Análise de Partidos e Fornecedores
if opcao == "Consulta 1":
    st.header("Consulta 1: Análise de Partidos com Fornecedores que Receberam acima de R$10.000")
    valor_minimo = st.slider("Escolha o valor mínimo de pagamento", min_value=0, max_value=50000, value=10000, step=1000)

    query1 = """
    SELECT
        p.NM_PARTIDO AS Nome_Partido,
        f.NM_FORNECEDOR AS Nome_Fornecedor,
        dp.VR_PAGAMENTO AS Valor_Pagamento
    FROM
        Partido p
        NATURAL JOIN Prestador pr
        NATURAL JOIN Despesa dp
        NATURAL JOIN Fornecedor f
    WHERE
        p.DS_TP_ESFERA_PARTIDARIA = 'Nacional'
        AND dp.VR_PAGAMENTO > ?
    """
    df1 = pd.read_sql_query(query1, conn, params=[valor_minimo])

    if not df1.empty:
        st.dataframe(df1)
        fornecedores_mais_receberam = (
            df1.groupby("Nome_Fornecedor")["Valor_Pagamento"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        # Gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        fornecedores_mais_receberam.plot(kind="barh", ax=ax, edgecolor="black", color="skyblue")
        ax.set_title("10 Fornecedores que Mais Receberam", fontsize=16)
        ax.set_xlabel("Montante Total Recebido (R$)", fontsize=12)
        ax.set_ylabel("Nome do Fornecedor", fontsize=12)
        ax.grid(axis="x", linestyle="--", alpha=0.7)
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 2":
    st.header("Consulta 2: Análise de Prestadores por Partido (SP)")

    # Criar a consulta SQL para carregar df2
    estado = st.sidebar.selectbox("Selecione o Estado", ["SP", "MG", "RJ", "BA"], index=0)
    query2 = f"""
    SELECT
        Pr.NR_CNPJ_PRESTADOR_CONTA AS CNPJ_Prestador,
        P.NM_PARTIDO AS Nome_Partido
    FROM
        Prestador Pr
        NATURAL JOIN Partido P
        NATURAL JOIN Local L
    WHERE
        L.SG_UF = '{estado}'
    """
    df2 = pd.read_sql_query(query2, conn)

    if not df2.empty:
        # Filtro de Partido
        partidos_disponiveis = df2["Nome_Partido"].unique()
        partidos_selecionados = st.sidebar.multiselect(
            "Selecione os partidos", partidos_disponiveis, default=partidos_disponiveis
        )

        # Filtro de CNPJ (Número de Prestadores)
        min_prestadores = st.sidebar.slider("Número mínimo de prestadores", min_value=1, max_value=int(df2["CNPJ_Prestador"].nunique()), value=1)
        max_prestadores = st.sidebar.slider("Número máximo de prestadores", min_value=1, max_value=int(df2["CNPJ_Prestador"].nunique()), value=20)

        # Filtro para mostrar Top N Partidos
        top_n = st.sidebar.slider("Quantos partidos deseja exibir?", min_value=1, max_value=10, value=5)

        # Aplicar o filtro de número de prestadores
        partido_prestadores = (
            df2.groupby("Nome_Partido")["CNPJ_Prestador"].nunique().reset_index()
        )
        partido_prestadores.rename(columns={"CNPJ_Prestador": "Numero_Prestadores"}, inplace=True)

        # Filtro para limitar Top N Partidos
        partido_prestadores = partido_prestadores.head(top_n)

        # Exibir a tabela
        st.dataframe(df2)

        # Gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(
            partido_prestadores["Nome_Partido"],
            partido_prestadores["Numero_Prestadores"],
            color="skyblue",
            edgecolor="black",
        )
        ax.set_title(f"Número de Prestadores por Partido ({estado})", fontsize=16)
        ax.set_xlabel("Número de Prestadores", fontsize=14)
        ax.set_ylabel("Partido", fontsize=14)
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")



elif opcao == "Consulta 3":
    st.header("Consulta 3: Análise de Fornecedores por Tipo e Valor de Pagamento")
    query3 = """
    SELECT
        F.NM_FORNECEDOR AS Nome_Fornecedor,
        F.DS_TP_FORNECEDOR AS Tipo_Fornecedor,
        D.VR_PAGAMENTO AS Valor_Pagamento,
        D.DT_PAGAMENTO AS Data_Pagamento
    FROM
        Fornecedor F
        NATURAL JOIN Despesa D
    ORDER BY
        D.VR_PAGAMENTO DESC;
    """
    df3 = pd.read_sql_query(query3, conn)

    if not df3.empty:
        st.dataframe(df3)
        fig, ax = plt.subplots(figsize=(12, 6))
        df3.groupby("Tipo_Fornecedor")["Valor_Pagamento"].sum().plot(
            kind="bar", ax=ax, color="skyblue", edgecolor="black"
        )
        ax.set_title("Total de Pagamentos por Tipo de Fornecedor", fontsize=16)
        ax.set_xlabel("Tipo de Fornecedor", fontsize=14)
        ax.set_ylabel("Montante Total (R$)", fontsize=14)
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 4":
    st.header("Consulta 4: Número de Prestadores por Partido")
    st.text("A consulta retorna uma lista de prestadores de contas com o nome do partido e a sigla do partido aos quais estão afiliados.")
    
    query4 = """
    SELECT
        Pr.NR_CNPJ_PRESTADOR_CONTA AS CNPJ_Prestador,
        P.NM_PARTIDO AS Nome_Partido,
        P.SG_PARTIDO AS Sigla_Partido
    FROM
        Prestador Pr
        NATURAL JOIN Partido P
    ORDER BY
        P.NM_PARTIDO;
    """
    df4 = pd.read_sql_query(query4, conn)

    if not df4.empty:
        st.write("### Resultados da Consulta 4")
        st.dataframe(df4.head(20))

        # Gráfico do Número de Prestadores por Partido
        partido_prestadores = df4.groupby('Sigla_Partido')['CNPJ_Prestador'].nunique().reset_index()
        partido_prestadores.rename(columns={'CNPJ_Prestador': 'Numero_Prestadores'}, inplace=True)
        partido_prestadores = partido_prestadores.sort_values(by='Numero_Prestadores', ascending=False)

        fig4, ax4 = plt.subplots(figsize=(12, 6))
        ax4.bar(partido_prestadores['Sigla_Partido'], partido_prestadores['Numero_Prestadores'], color="skyblue", edgecolor="black")
        ax4.set_title('Número de Prestadores por Partido (BR)', fontsize=16)
        ax4.set_xlabel('Partido', fontsize=14)
        ax4.set_ylabel('Número de Prestadores', fontsize=14)
        ax4.grid(axis="y", linestyle="--", alpha=0.7)
        ax4.tick_params(axis='x', rotation=45)
        st.pyplot(fig4)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 5":
    st.header("Consulta 5: Municípios com mais prestadores")
    
    query5 = """
    SELECT
        Pr.NR_CNPJ_PRESTADOR_CONTA AS CNPJ_Prestador,
        L.NM_MUNICIPIO AS Nome_Municipio,
        L.SG_UF AS Estado
    FROM
        Prestador Pr
        NATURAL JOIN Local L
    ORDER BY
        L.NM_MUNICIPIO;
    """
    df5 = pd.read_sql_query(query5, conn)

    if not df5.empty:
        st.subheader("Tabela de Prestadores por Município")
        st.dataframe(df5)

        municipality_counts = df5['Nome_Municipio'].value_counts()

        # Gráfico
        st.subheader("Gráfico: Municípios com mais prestadores")
        top_n = 5
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.bar(municipality_counts.index[:top_n], municipality_counts.values[:top_n], color="skyblue", edgecolor="black")
        ax.set_xlabel('Município')
        ax.set_ylabel('Número de Prestadores')
        ax.set_title(f'Top {top_n} Municípios com Mais Prestadores')
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        plt.xticks(rotation=45, ha='right')
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

# Consulta 6: Média de gastos por partido
elif opcao == "Consulta 6":

    st.header("Consulta 6: Média de gastos por partido")

    query6 = """
    SELECT
        PT.SG_PARTIDO AS Sigla_Partido,
        PT.NM_PARTIDO AS Nome_Partido,
        AVG(D.VR_PAGAMENTO) AS Media_Gastos
    FROM
        Despesa D
        NATURAL JOIN Prestador P
        NATURAL JOIN Partido PT
    GROUP BY
        PT.SG_PARTIDO, PT.NM_PARTIDO
    ORDER BY
        Media_Gastos DESC;
    """

    # Executar consulta SQL
    df6 = pd.read_sql_query(query6, conn)

    # Verificar se há dados
    if not df6.empty:
        # Exibir tabela
        st.subheader("Tabela de Média de Gastos por Partido")
        st.dataframe(df6)

        # Gráfico
        st.subheader("Gráfico: Média de Gastos por Partido")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(df6['Sigla_Partido'], df6['Media_Gastos'], color="skyblue", edgecolor="black")
        ax.set_xlabel('Partido Político', fontsize=14)
        ax.set_ylabel('Média de Gastos (R$)', fontsize=14)
        ax.set_title('Média de Gastos por Partido', fontsize=16)
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        plt.xticks(rotation=45, ha='right')
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 7":

    st.header("Consulta 7: Contratos por partido e município")

    # Consulta SQL
    query7 = """
    SELECT
        L.NM_MUNICIPIO AS Nome_Municipio,
        P.SG_PARTIDO AS Sigla_Partido,
        F.NM_FORNECEDOR AS Nome_Fornecedor,
        COUNT(D.NR_CPF_CNPJ_FORNECEDOR) AS Quantidade_Contratos
    FROM
        Despesa D
        NATURAL JOIN Prestador P
        NATURAL JOIN Fornecedor F
        NATURAL JOIN Local L
    GROUP BY
        L.NM_MUNICIPIO, P.SG_PARTIDO, F.NM_FORNECEDOR
    ORDER BY
        Quantidade_Contratos DESC;
    """

    # Executar consulta SQL
    df7 = pd.read_sql_query(query7, conn)

    # Verificar se há dados
    if not df7.empty:
        # Exibir tabela
        st.subheader("Tabela de Contratos por Município e Partido")
        st.dataframe(df7)

        # Agrupamento para análise gráfica
        partidos = df7.groupby("Sigla_Partido")["Quantidade_Contratos"].sum().reset_index()
        partidos = partidos.sort_values(by="Quantidade_Contratos", ascending=False)
        top_partidos = partidos.head(10)

        # Gráfico
        st.subheader("Gráfico: Partidos com mais contratos")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(top_partidos["Sigla_Partido"], top_partidos["Quantidade_Contratos"], color="skyblue", edgecolor="black")
        ax.set_xlabel("Quantidade de Contratos", fontsize=14)
        ax.set_ylabel("Partido Político", fontsize=14)
        ax.set_title("10 Partidos Políticos com Mais Contratos", fontsize=16)
        ax.grid(axis="x", linestyle="--", alpha=0.7)
        ax.invert_yaxis()  # Inverter eixo para melhor visualização
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 8":

    st.header("Consulta 8: Maiores despesas por fornecedor em 2024")

    # Consulta SQL
    query8 = """
    SELECT
        F.NM_FORNECEDOR AS Nome_Fornecedor,
        COUNT(D.NR_CPF_CNPJ_FORNECEDOR) AS Numero_Pagamentos,
        SUM(D.VR_PAGAMENTO) AS Total_Despesas
    FROM
        Despesa D
        NATURAL JOIN Fornecedor F
    WHERE
        D.DT_PAGAMENTO BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY
        F.NM_FORNECEDOR
    ORDER BY
        Total_Despesas DESC;
    """

    # Executar consulta SQL
    df8 = pd.read_sql_query(query8, conn)

    # Verificar se há dados
    if not df8.empty:
        # Criar coluna de despesas em milhões
        df8['Total_Despesas_Milhoes'] = df8['Total_Despesas'] / 1_000_000
        top_10_despesas = df8.nlargest(10, 'Total_Despesas_Milhoes')

        # Exibir tabela
        st.subheader("Tabela de Despesas por Fornecedor")
        st.dataframe(df8)

        # Gráfico
        st.subheader("Gráfico: 10 Maiores Despesas por Fornecedor (2024)")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(top_10_despesas['Nome_Fornecedor'], top_10_despesas['Total_Despesas_Milhoes'], color="skyblue", edgecolor="black")
        ax.set_title('10 Maiores Despesas (2024) - Milhões de R$', fontsize=16)
        ax.set_xlabel('Despesa (Milhões de R$)', fontsize=14)
        ax.set_ylabel('Fornecedor', fontsize=14)
        ax.grid(axis="x", linestyle="--", alpha=0.7)
        ax.invert_yaxis()  # Inverter eixo para melhorar a legibilidade
        plt.tight_layout()
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 9":

    st.header("Consulta 9: Total de despesas por partido em MG (Jan 2024)")

    # Consulta SQL
    query9 = """
    SELECT
        Pt.NM_PARTIDO AS Partido,
        SUM(D.VR_PAGAMENTO) AS Total_Despesas
    FROM
        Despesa D
        NATURAL JOIN Prestador P
        NATURAL JOIN Fornecedor F
        NATURAL JOIN Local L
        NATURAL JOIN Partido Pt
    WHERE
        D.DT_PAGAMENTO BETWEEN '2024-01-01' AND '2024-01-31' AND L.SG_UF = 'MG'
    GROUP BY
        Pt.NM_PARTIDO
    ORDER BY
        Total_Despesas DESC;
    """

    # Executar consulta SQL
    df9 = pd.read_sql_query(query9, conn)

    # Verificar se há dados
    if not df9.empty:
        # Exibir tabela
        st.subheader("Tabela de Despesas por Partido (Jan 2024 - MG)")
        st.dataframe(df9)

        # Gráfico
        st.subheader("Gráfico: Total de Despesas por Partido (Jan 2024 - MG)")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(df9['Partido'], df9['Total_Despesas'], color="lightcoral", edgecolor="black")
        ax.set_title('Total de Despesas por Partido (Jan 2024 - MG)', fontsize=16)
        ax.set_xlabel('Total de Despesas (R$)', fontsize=14)
        ax.set_ylabel('Partido', fontsize=14)
        ax.grid(axis="x", linestyle="--", alpha=0.7)
        ax.invert_yaxis() 
        plt.tight_layout()
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")

elif opcao == "Consulta 10":

    st.header("Consulta 10: Municípios com maior total de despesas em 2024")

    # Consulta SQL
    query10 = """
    SELECT
        L.NM_MUNICIPIO AS Municipio,
        SUM(D.VR_PAGAMENTO) AS Total_Despesas
    FROM
        Despesa D
        NATURAL JOIN Prestador P
        NATURAL JOIN Local L
    WHERE
        D.DT_PAGAMENTO BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY
        L.NM_MUNICIPIO
    ORDER BY
        Total_Despesas DESC
    LIMIT 5;
    """

    # Executar consulta SQL
    df10 = pd.read_sql_query(query10, conn)

    # Verificar se há dados
    if not df10.empty:
        # Exibir tabela
        st.subheader("Tabela de Municípios com Maior Total de Despesas")
        st.dataframe(df10)

        # Gráfico
        st.subheader("Gráfico: Distribuição de Despesas entre os 5 Municípios com Maior Total (2024)")
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.pie(
            df10['Total_Despesas'],
            labels=df10['Municipio'],
            autopct='%1.1f%%',
            startangle=90,
            colors=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0'],
            pctdistance=0.85
        )
        ax.set_title('Distribuição de Despesas entre os 5 Municípios com Maior Total de Despesas (2024)', fontsize=16)
        plt.axis('equal')
        st.pyplot(fig)
    else:
        st.write("Nenhum dado encontrado.")
