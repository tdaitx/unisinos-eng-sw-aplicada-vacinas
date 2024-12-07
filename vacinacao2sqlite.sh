#!/bin/bash -eu

if [ "x${1-}" == "x" ]; then
	echo "Usage: $0 file.zip [file2.zip] ..."
	exit 1
fi


filtro_colunas="co_municipio_paciente
no_municipio_paciente
no_pais_paciente
sg_uf_paciente
sg_vacina
dt_vacina
ds_dose_vacina
ds_vacina
nu_idade_paciente"

SQL_DB="vacinacao.sqlite3"


{
	echo 'CREATE TABLE IF NOT EXISTS vacinacao ('
	for nome_coluna in $filtro_colunas; do
		case $nome_coluna in
			co_*) tipo_coluna='NUMERIC';;
			nu_*) tipo_coluna='INTEGER';;
			*) tipo_coluna='STRING';;
		esac;
		echo "\"$nome_coluna\" $tipo_coluna";
	done | paste -sd ','
	echo ");"
} | tee /dev/stderr | sqlite3 vacinacao.sqlite3 ".read '|cat -'"

for f in "$@"; do
	echo "Processando arquivo: $f"
	colunas_arquivo=$(unzip -p $1 | head -n1 | tr ';' '\n')
	colunas_encontradas=$(echo "$colunas_arquivo" | grep -nxFf <(echo "$filtro_colunas"))
	colunas_numeradas=$(echo "$colunas_encontradas" | cut -d: -f1 | paste -sd,)
	if [[ $(echo "$filtro_colunas" | wc -l) -ne $(echo "$colunas_encontradas" | wc -l) ]]; then
		echo "Erro: nem todas as colunas puderam ser extraídas"
		echo "Colunas solicitadas:"
		echo "$filtro_colunas"
		echo "Colunas encontradas:"
		echo "$colunas_encontradas"
		exit 1
	fi
	echo "Gravando colunas:"
	echo "$colunas_encontradas"
	unzip -p "$f" | cut -s -d';' -f "$colunas_numeradas" | iconv -f ISO-8859-1 -t UTF-8 | tail -n+2 | sqlite3 --csv --separator ";" vacinacao.sqlite3 ".import '|cat -' vacinacao"
done

sqlite3 vacinacao.sqlite3 << EOF
.tables
.schema
select count(*) from vacinacao;
delete from vacinacao where no_pais_paciente!="BRASIL";
delete from vacinacao where co_municipio_paciente=999999 or no_municipio_paciente="INVALIDO";
delete from vacinacao where nu_idade_paciente="";
delete from vacinacao where sg_uf_paciente="" or no_municipio_paciente="" or co_municipio_paciente="";
delete from vacinacao where ds_vacina="" OR sg_vacina="";
select count(*) from vacinacao;
EOF
