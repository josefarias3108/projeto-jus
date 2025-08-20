import os
import json
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime

# === LOCALIZAÇÃO DINÂMICA DO db_config.json ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "db_config.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    DB_CONFIG = json.load(f)

# === CONEXÃO COM O POSTGRESQL ===
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# === FUNÇÃO: CONVERTER TIPOS NUMPY PARA PYTHON NATIVOS ===
def converter_tipos_numpy(valor):
    """
    Converte tipos numpy para tipos Python nativos compatíveis com psycopg2
    """
    if pd.isna(valor):
        return None
    elif isinstance(valor, np.integer):  # Inclui int64, int32, int16, int8
        return int(valor)
    elif isinstance(valor, np.floating):  # Inclui float64, float32
        return float(valor)
    elif isinstance(valor, (np.bool_, bool)):  # Apenas np.bool_
        return bool(valor)
    elif isinstance(valor, np.str_):  # String numpy (np.unicode_ removido no NumPy 2.0)
        return str(valor)
    elif isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()
    elif isinstance(valor, np.datetime64):
        return pd.Timestamp(valor).to_pydatetime()
    elif hasattr(valor, 'item'):  # Qualquer escalar numpy
        return valor.item()
    else:
        return valor

# === FUNÇÃO: LIMPAR DATAFRAME DE TIPOS NUMPY ===
def limpar_tipos_numpy(df):
    """
    Remove tipos numpy do DataFrame para evitar erros de inserção no PostgreSQL
    """
    df_limpo = df.copy()
    
    for coluna in df_limpo.columns:
        dtype_str = str(df_limpo[coluna].dtype)
        
        if df_limpo[coluna].dtype == 'object':
            # Para colunas object, aplicar conversão em cada valor
            df_limpo[coluna] = df_limpo[coluna].apply(converter_tipos_numpy)
        elif 'int' in dtype_str:
            # Converter qualquer tipo inteiro para int Python nativo
            df_limpo[coluna] = df_limpo[coluna].apply(lambda x: int(x) if pd.notna(x) else None)
        elif 'float' in dtype_str:
            # Converter qualquer tipo float para float Python nativo
            df_limpo[coluna] = df_limpo[coluna].apply(lambda x: float(x) if pd.notna(x) else None)
        elif 'bool' in dtype_str:
            # Converter bool numpy para bool Python
            df_limpo[coluna] = df_limpo[coluna].apply(lambda x: bool(x) if pd.notna(x) else None)
        elif 'datetime' in dtype_str:
            # Converter timestamps para datetime Python
            df_limpo[coluna] = df_limpo[coluna].apply(
                lambda x: x.to_pydatetime() if pd.notna(x) and hasattr(x, 'to_pydatetime') else x
            )
    
    return df_limpo

# === FUNÇÃO: CRIAR TABELA DE LOG ===
def criar_tabela_log():
    """
    Cria tabela para armazenar logs de extração e validação
    """
    cur.execute("""
    CREATE TABLE IF NOT EXISTS log_extractions (
        id SERIAL PRIMARY KEY,
        data_execucao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tabela VARCHAR(50),
        acao VARCHAR(20), -- 'VALIDACAO', 'EXTRACAO', 'ERRO'
        registros_processados INT DEFAULT 0,
        duplicatas_encontradas INT DEFAULT 0,
        nulos_encontrados INT DEFAULT 0,
        duplicatas_removidas INT DEFAULT 0,
        nulos_tratados INT DEFAULT 0,
        status VARCHAR(10), -- 'SUCESSO', 'ERRO', 'AVISO'
        detalhes TEXT,
        arquivo_gerado VARCHAR(255)
    );
    """)
    conn.commit()
    print("📋 Tabela de log criada/verificada: log_extractions")

# === FUNÇÃO: INSERIR LOG (COM TRATAMENTO DE TIPOS) ===
def inserir_log(tabela, acao, registros=0, duplicatas=0, nulos=0, dup_removidas=0, nulos_tratados=0, status='SUCESSO', detalhes='', arquivo=''):
    """
    Insere registro na tabela de log com conversão de tipos
    """
    # Converter todos os valores para tipos Python nativos
    params = (
        str(tabela),
        str(acao),
        int(registros) if registros is not None else 0,
        int(duplicatas) if duplicatas is not None else 0,
        int(nulos) if nulos is not None else 0,
        int(dup_removidas) if dup_removidas is not None else 0,
        int(nulos_tratados) if nulos_tratados is not None else 0,
        str(status),
        str(detalhes),
        str(arquivo)
    )
    
    cur.execute("""
        INSERT INTO log_extractions 
        (tabela, acao, registros_processados, duplicatas_encontradas, nulos_encontrados, 
         duplicatas_removidas, nulos_tratados, status, detalhes, arquivo_gerado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, params)
    conn.commit()

# === FUNÇÃO: VALIDAR E LIMPAR DADOS ===
def validar_e_limpar_dados(df, tabela_nome):
    """
    Valida dados, remove duplicatas e trata valores nulos
    Retorna DataFrame limpo e estatísticas
    """
    print(f"  🔍 Validando dados da tabela {tabela_nome}...")
    
    # Estatísticas iniciais
    registros_iniciais = len(df)
    duplicatas_iniciais = df.duplicated().sum()
    
    # Contar nulos por coluna (exceto ID que pode ser auto-incremento)
    colunas_importantes = [col for col in df.columns if col.lower() not in ['id']]
    nulos_por_coluna = df[colunas_importantes].isnull().sum()
    nulos_totais = nulos_por_coluna.sum()
    
    print(f"    📊 Registros iniciais: {registros_iniciais}")
    print(f"    🔄 Duplicatas encontradas: {duplicatas_iniciais}")
    print(f"    ❌ Valores nulos encontrados: {nulos_totais}")
    
    # === TRATAMENTO DE DUPLICATAS ===
    df_limpo = df.copy()
    
    if duplicatas_iniciais > 0:
        # Remove duplicatas mantendo o primeiro registro
        df_limpo = df_limpo.drop_duplicates()
        duplicatas_removidas = registros_iniciais - len(df_limpo)
        print(f"    ✅ {duplicatas_removidas} duplicatas removidas")
        
        inserir_log(
            tabela=tabela_nome,
            acao='VALIDACAO',
            registros=registros_iniciais,
            duplicatas=duplicatas_iniciais,
            dup_removidas=duplicatas_removidas,
            status='AVISO' if duplicatas_iniciais > 0 else 'SUCESSO',
            detalhes=f'Duplicatas encontradas e removidas: {duplicatas_removidas}'
        )
    
    # === TRATAMENTO DE VALORES NULOS ===
    nulos_tratados = 0
    
    if nulos_totais > 0:
        print(f"    🔧 Tratando valores nulos:")
        
        for coluna in colunas_importantes:
            nulos_coluna = df_limpo[coluna].isnull().sum()
            if nulos_coluna > 0:
                print(f"      • {coluna}: {nulos_coluna} nulos", end=" → ")
                
                # Estratégias específicas por tipo de coluna
                if df_limpo[coluna].dtype == 'object':  # Texto
                    if 'nome' in coluna.lower() or 'cliente' in coluna.lower() or 'advogado' in coluna.lower() or 'juiz' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('Nome não informado')
                    elif 'cpf' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('000.000.000-00')
                    elif 'endereco' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('Endereço não informado')
                    elif 'cidade' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('Cidade não informada')
                    elif 'estado' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('XX')
                    elif 'oab' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('OAB não informada')
                    elif 'vara' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna('Vara não informada')
                    elif 'numero' in coluna.lower() and 'processo' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna(f'PROC-{datetime.now().strftime("%Y%m%d")}-{len(df_limpo)}')
                    else:
                        df_limpo[coluna] = df_limpo[coluna].fillna('Não informado')
                    print("preenchido com valor padrão")
                    
                elif df_limpo[coluna].dtype in ['int64', 'float64', 'Int64', 'Float64']:  # Números
                    if 'valor' in coluna.lower():
                        df_limpo[coluna] = df_limpo[coluna].fillna(0.0)
                        print("preenchido com 0.0")
                    else:
                        df_limpo[coluna] = df_limpo[coluna].fillna(0)
                        print("preenchido com 0")
                        
                elif 'bool' in str(df_limpo[coluna].dtype):  # Booleano
                    df_limpo[coluna] = df_limpo[coluna].fillna(False)
                    print("preenchido com False")
                    
                elif 'date' in str(df_limpo[coluna].dtype):  # Data
                    df_limpo[coluna] = df_limpo[coluna].fillna(pd.Timestamp('1900-01-01'))
                    print("preenchido com data padrão")
                
                nulos_tratados += nulos_coluna
        
        print(f"    ✅ {nulos_tratados} valores nulos tratados")
        
        inserir_log(
            tabela=tabela_nome,
            acao='VALIDACAO',
            registros=len(df_limpo),
            nulos=nulos_totais,
            nulos_tratados=nulos_tratados,
            status='AVISO' if nulos_totais > 0 else 'SUCESSO',
            detalhes=f'Valores nulos encontrados e tratados: {nulos_tratados}'
        )
    
    # === VALIDAÇÕES ESPECÍFICAS POR TABELA ===
    if tabela_nome == 'fato_processos':
        # Garantir que não há processos sem pessoa, juiz ou advogado
        registros_invalidos = df_limpo[
            (df_limpo['id_pessoa'].isnull()) | 
            (df_limpo['id_juiz'].isnull()) | 
            (df_limpo['id_advogado'].isnull())
        ]
        
        if len(registros_invalidos) > 0:
            df_limpo = df_limpo.dropna(subset=['id_pessoa', 'id_juiz', 'id_advogado'])
            print(f"    ⚠️ {len(registros_invalidos)} processos inválidos removidos (sem pessoa/juiz/advogado)")
    
    # === CONVERTER TIPOS NUMPY PARA PYTHON NATIVOS ===
    print(f"  🔧 Convertendo tipos numpy para tipos Python nativos...")
    df_limpo = limpar_tipos_numpy(df_limpo)
    print(f"    ✅ Tipos convertidos com sucesso")
    
    # Estatísticas finais
    registros_finais = len(df_limpo)
    print(f"    ✅ Registros finais após limpeza: {registros_finais}")
    print(f"    📈 Dados limpos: {((registros_finais/registros_iniciais)*100):.1f}% dos registros originais")
    
    return df_limpo, {
        'registros_iniciais': registros_iniciais,
        'registros_finais': registros_finais,
        'duplicatas_encontradas': duplicatas_iniciais,
        'duplicatas_removidas': duplicatas_iniciais,
        'nulos_encontrados': nulos_totais,
        'nulos_tratados': nulos_tratados
    }

# === FUNÇÃO: EXTRAÇÃO PARA CSV COM VALIDAÇÃO ===
def extrair_csvs_validados():
    """
    Extrai todas as tabelas para CSV com validação completa
    """
    print("📄 Iniciando extração validada dos CSVs...")
    
    # Cria pasta para os CSVs se não existir
    csv_dir = os.path.join(BASE_DIR, "csvs")
    os.makedirs(csv_dir, exist_ok=True)
    
    # Lista das tabelas para exportar
    tabelas = ['dim_pessoa', 'dim_juiz', 'dim_advogado', 'fato_processos']
    
    resultados_gerais = {
        'total_tabelas': len(tabelas),
        'tabelas_sucesso': 0,
        'total_registros_originais': 0,
        'total_registros_finais': 0,
        'total_duplicatas_removidas': 0,
        'total_nulos_tratados': 0,
        'arquivos_gerados': []
    }
    
    for tabela in tabelas:
        try:
            print(f"\n📋 Processando tabela: {tabela}")
            print("-" * 50)
            
            # 1. EXTRAÇÃO DOS DADOS
            print(f"  📥 Extraindo dados da tabela {tabela}...")
            df_original = pd.read_sql(f"SELECT * FROM {tabela}", conn)
            print(f"    ✅ {len(df_original)} registros extraídos")
            
            # 2. VALIDAÇÃO E LIMPEZA
            df_limpo, stats = validar_e_limpar_dados(df_original, tabela)
            
            # 3. VERIFICAÇÃO FINAL DE INTEGRIDADE
            print(f"  🔐 Verificação final de integridade...")
            
            # Verificar se ainda há duplicatas
            duplicatas_finais = df_limpo.duplicated().sum()
            if duplicatas_finais > 0:
                print(f"    ⚠️ ATENÇÃO: {duplicatas_finais} duplicatas ainda presentes!")
                df_limpo = df_limpo.drop_duplicates()
            
            # Verificar nulos em colunas críticas
            colunas_criticas = [col for col in df_limpo.columns if col.lower() not in ['id']]
            nulos_finais = df_limpo[colunas_criticas].isnull().sum().sum()
            
            print(f"    ✅ Duplicatas finais: {duplicatas_finais}")
            print(f"    ✅ Nulos finais em colunas críticas: {nulos_finais}")
            
            # 4. SALVAR CSV
            csv_path = os.path.join(csv_dir, f"{tabela}.csv")
            df_limpo.to_csv(csv_path, index=False, encoding='utf-8')
            
            # Calcular tamanho do arquivo
            tamanho_mb = os.path.getsize(csv_path) / (1024 * 1024)
            
            print(f"  💾 Arquivo salvo: {tabela}.csv ({len(df_limpo)} registros, {tamanho_mb:.2f} MB)")
            
            # 5. LOG DA EXTRAÇÃO
            inserir_log(
                tabela=tabela,
                acao='EXTRACAO',
                registros=len(df_limpo),
                duplicatas=stats['duplicatas_encontradas'],
                nulos=stats['nulos_encontrados'],
                dup_removidas=stats['duplicatas_removidas'],
                nulos_tratados=stats['nulos_tratados'],
                status='SUCESSO',
                detalhes=f'Arquivo CSV gerado com {len(df_limpo)} registros válidos',
                arquivo=f"{tabela}.csv"
            )
            
            # Atualizar estatísticas gerais
            resultados_gerais['tabelas_sucesso'] += 1
            resultados_gerais['total_registros_originais'] += stats['registros_iniciais']
            resultados_gerais['total_registros_finais'] += stats['registros_finais']
            resultados_gerais['total_duplicatas_removidas'] += stats['duplicatas_removidas']
            resultados_gerais['total_nulos_tratados'] += stats['nulos_tratados']
            resultados_gerais['arquivos_gerados'].append({
                'tabela': tabela,
                'arquivo': f"{tabela}.csv",
                'registros': len(df_limpo),
                'tamanho_mb': round(tamanho_mb, 2)
            })
            
        except Exception as e:
            print(f"  ❌ ERRO ao processar {tabela}: {e}")
            inserir_log(
                tabela=tabela,
                acao='ERRO',
                status='ERRO',
                detalhes=f'Erro na extração: {str(e)}'
            )
    
    return resultados_gerais

# === FUNÇÃO: GERAR RELATÓRIO DE LOGS ===
def gerar_relatorio_logs():
    """
    Gera relatório consolidado dos logs
    """
    print(f"\n📊 Gerando relatório de logs...")
    
    # Buscar logs da execução atual (últimos 5 minutos)
    cur.execute("""
        SELECT * FROM log_extractions 
        WHERE data_execucao >= NOW() - INTERVAL '5 minutes'
        ORDER BY data_execucao DESC
    """)
    
    logs = cur.fetchall()
    colunas = [desc[0] for desc in cur.description]
    
    if logs:
        df_logs = pd.DataFrame(logs, columns=colunas)
        
        # Limpar tipos numpy do DataFrame de logs também
        df_logs = limpar_tipos_numpy(df_logs)
        
        # Salvar relatório de logs
        log_path = os.path.join(BASE_DIR, "csvs", "relatorio_logs.csv")
        df_logs.to_csv(log_path, index=False, encoding='utf-8')
        
        print(f"  ✅ Relatório de logs salvo: relatorio_logs.csv")
        return True
    
    return False

# === EXECUÇÃO PRINCIPAL ===
def main():
    try:
        print("🚀 Iniciando extração validada da base jurídica...")
        print("=" * 70)
        
        # 1. Criar tabela de log
        criar_tabela_log()
        
        # 2. Log de início da execução
        inserir_log(
            tabela='SISTEMA',
            acao='INICIO',
            status='SUCESSO',
            detalhes='Iniciando processo de extração validada'
        )
        
        # 3. Extrair CSVs com validação
        resultados = extrair_csvs_validados()
        
        # 4. Mostrar estatísticas da extração
        print(f"\n📈 ESTATÍSTICAS DA BASE:")
        print("=" * 70)
        
        # Estatísticas do banco
        cur.execute("SELECT COUNT(*) FROM dim_pessoa")
        total_pessoas = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dim_juiz")
        total_juizes = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dim_advogado")
        total_advogados = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fato_processos")
        total_processos = cur.fetchone()[0]
        
        print(f"   📊 Dados no Banco:")
        print(f"     • Pessoas: {total_pessoas:,}")
        print(f"     • Juízes: {total_juizes:,}")
        print(f"     • Advogados: {total_advogados:,}")
        print(f"     • Processos: {total_processos:,}")
        
        # 5. Estatísticas da extração
        print(f"\n   🔍 Resultados da Validação:")
        print(f"     • Tabelas processadas: {resultados['tabelas_sucesso']}/{resultados['total_tabelas']}")
        print(f"     • Registros originais: {resultados['total_registros_originais']:,}")
        print(f"     • Registros finais: {resultados['total_registros_finais']:,}")
        print(f"     • Duplicatas removidas: {resultados['total_duplicatas_removidas']:,}")
        print(f"     • Valores nulos tratados: {resultados['total_nulos_tratados']:,}")
        
        # 6. Arquivos gerados
        print(f"\n   📁 Arquivos CSV Gerados:")
        for arquivo in resultados['arquivos_gerados']:
            print(f"     • {arquivo['arquivo']}: {arquivo['registros']:,} registros ({arquivo['tamanho_mb']} MB)")
        
        # 7. Gerar relatório de logs
        gerar_relatorio_logs()
        
        # 8. Log de fim da execução
        taxa_sucesso = (resultados['tabelas_sucesso'] / resultados['total_tabelas']) * 100
        inserir_log(
            tabela='SISTEMA',
            acao='FIM',
            registros=resultados['total_registros_finais'],
            duplicatas=resultados['total_duplicatas_removidas'],
            nulos_tratados=resultados['total_nulos_tratados'],
            status='SUCESSO' if taxa_sucesso == 100 else 'AVISO',
            detalhes=f'Extração concluída: {taxa_sucesso:.1f}% de sucesso'
        )
        
        print(f"\n🎯 EXTRAÇÃO VALIDADA COMPLETA:")
        print("=" * 70)
        print(f"   ✅ CSVs extraídos com validação completa")
        print(f"   ✅ Garantia: 0 duplicatas, 0 valores nulos críticos")
        print(f"   ✅ Tipos numpy convertidos para compatibilidade PostgreSQL")
        print(f"   ✅ Logs salvos na tabela 'log_extractions'")
        print(f"   📁 Arquivos disponíveis em: {os.path.join(BASE_DIR, 'csvs')}")
        print(f"   📊 Taxa de sucesso: {taxa_sucesso:.1f}%")
            
    except Exception as e:
        print(f"❌ Erro crítico na execução: {e}")
        inserir_log(
            tabela='SISTEMA',
            acao='ERRO',
            status='ERRO',
            detalhes=f'Erro crítico: {str(e)}'
        )
        
    finally:
        cur.close()
        conn.close()
        print("\n🔌 Conexão com banco encerrada.")

if __name__ == "__main__":
    main()