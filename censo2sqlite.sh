#!/bin/bash -xeu

function filtrar_idades() {
	sed -e "s/Menos de 1 ano/0/" -e "s/ anos ou mais//" -e "s/ anos\?//"
}

function filtrar_sexo() {
	sed -e "s/Homens/H/" -e "s/Mulheres/M/"
}

function separar_estado() {
	sed -E 's/ \(([A-Z]{2})\)/\\";\"\1/'
}

function substituir_por_zeros() {
	sed 's/"-"/0/'
}

sqlite3 vacinacao.sqlite3 "DROP TABLE IF EXISTS censo2022regiao;"
sqlite3 vacinacao.sqlite3 "CREATE TABLE censo2022regiao ( \"no_regiao\" STRING, \"nu_idade\" INTEGER, \"co_sexo\" STRING, \"nu_populacao\" INTEGER);"
tail -n+6 arquivos_censo/censo2022-grande-regiao-todas-idades-sexo.csv | head -n1010 | sed 's/\r//' | filtrar_idades | filtrar_sexo | sqlite3 --csv --separator ";" vacinacao.sqlite3 ".import '|cat -' censo2022regiao"

sqlite3 vacinacao.sqlite3 "DROP TABLE IF EXISTS censo2022uf;"
sqlite3 vacinacao.sqlite3 "CREATE TABLE censo2022uf ( \"co_uf\" INTEGER, \"no_uf\" STRING, \"nu_idade\" INTEGER, \"co_sexo\" STRING, \"nu_populacao\" INTEGER);"
tail -n+6 arquivos_censo/censo2022-uf-todas-idades-sexo.csv | head -n5454 | sed 's/\r//' | filtrar_idades | filtrar_sexo | sqlite3 --csv --separator ";" vacinacao.sqlite3 ".import '|cat -' censo2022uf"

sqlite3 vacinacao.sqlite3 "DROP TABLE IF EXISTS censo2022municipio;"
sqlite3 vacinacao.sqlite3 "CREATE TABLE censo2022municipio ( \"co_municipio\" INTEGER, \"no_municipio\" STRING, \"sg_uf\" STRING, \"nu_idade\" INTEGER, \"co_sexo\" STRING, \"nu_populacao\" INTEGER);"
tail -n+6 arquivos_censo/censo2022-municipio-todas-idades-sexo.csv | head -n1125140 | sed 's/\r//' | filtrar_idades | filtrar_sexo | separar_estado | substituir_por_zeros | sqlite3 --csv --separator ";" vacinacao.sqlite3 ".import '|cat -' censo2022municipio"


### VIEWS
# população do censo por regiao/idade
sqlite3 vacinacao.sqlite3 "CREATE VIEW IF NOT EXISTS censo2022regiao_idade (no_regiao, nu_idade, nu_populacao) as SELECT no_regiao, nu_idade, sum(nu_populacao) as nu_populacao FROM censo2022regiao GROUP BY no_regiao, nu_idade;"

# população do censo por uf/idade
sqlite3 vacinacao.sqlite3 "CREATE VIEW IF NOT EXISTS censo2022uf_idade (sg_uf, nu_idade, nu_populacao) as SELECT sg_uf, nu_idade, sum(nu_populacao) as nu_populacao FROM censo2022uf c, regiao_uf r WHERE r.no_uf=c.no_uf GROUP BY sg_uf, nu_idade;"

# Dengue: doses aplicadas por ano/regiao/tipo dose/idade
sqlite3 vacinacao.sqlite3 "CREATE VIEW IF NOT EXISTS dengue_ano_regiao_dose_idade (nu_ano, no_regiao, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) as SELECT printf no_regiao, \"Dengue\", ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas FROM vacinacao, regiao_uf WHERE sg_uf=sg_uf_paciente AND nu_idade_paciente>=10 AND nu_idade_paciente<=14 AND (sg_vacina='Dengue' OR sg_vacina='DNG') AND (ds_dose_vacina='1ª Dose' OR ds_dose_vacina='2ª Dose') GROUP BY no_regiao, ds_dose_vacina, nu_idade_paciente;"

# doses 2024 MenACWY por regiao/tipo dose/idade
sqlite3 vacinacao.sqlite3 "CREATE VIEW IF NOT EXISTS vacinacaodoses_menacwy_regiao_dose_idade (no_regiao, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) as SELECT no_regiao, sg_vacina, ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas FROM vacinacao, regiao_uf WHERE sg_uf=sg_uf_paciente AND nu_idade_paciente>=11 AND nu_idade_paciente<=14 AND (sg_vacina='MenACWY') AND (ds_dose_vacina='Dose' OR ds_dose_vacina='Reforço' OR ds_dose_vacina='Única') GROUP BY no_regiao, ds_dose_vacina, nu_idade_paciente;"

# doses 2024 HPV4 por regiao/tipo dose/idade
sqlite3 vacinacao.sqlite3 "CREATE VIEW IF NOT EXISTS vacinacaodoses_hpv4_regiao_dose_idade (no_regiao, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) as SELECT no_regiao, sg_vacina, ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas FROM vacinacao, regiao_uf WHERE sg_uf=sg_uf_paciente AND nu_idade_paciente>=9 AND nu_idade_paciente<=14 AND (sg_vacina='HPV4') AND (ds_dose_vacina='1ª Dose' OR ds_dose_vacina='2ª Dose' OR ds_dose_vacina='Única') GROUP BY no_regiao, ds_dose_vacina, nu_idade_paciente;"


# MenACWY: doses anuais por uf/tipo dose/idade
sqlite3 vacinacao.sqlite3 \
"CREATE VIEW IF NOT EXISTS
	menacwy_uf_dose_idade (sg_uf, nu_ano, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) AS
	SELECT sg_uf_paciente, nu_ano, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas
	FROM vacinacao
	WHERE nu_idade_paciente>=11 AND nu_idade_paciente<=14
	AND sg_vacina='MenACWY'
	AND (ds_dose_vacina='Dose' OR ds_dose_vacina='Reforço' OR ds_dose_vacina='Única')
	GROUP BY sg_uf_paciente, nu_ano,ds_dose_vacina, nu_idade_paciente;"

# Dengue: doses mensais por regiao/tipo de dose/idade
sqlite3 vacinacao.sqlite3 \
"CREATE VIEW IF NOT EXISTS
	dengue_ano_mes_regiao_dose_idade (no_regiao, nu_ano, nu_mes, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) AS
	SELECT no_regiao, strftime('%Y', dt_vacina) as nu_ano, strftime('%m', dt_vacina) as nu_mes, \"Dengue\",
	       ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas
	FROM vacinacao, regiao_uf
	WHERE sg_uf=sg_uf_paciente
	AND nu_idade_paciente>=10
	AND nu_idade_paciente<=14
	AND (sg_vacina='Dengue' OR sg_vacina='DNG') \
	AND (ds_dose_vacina='1ª Dose' OR ds_dose_vacina='2ª Dose')
	GROUP BY no_regiao, nu_ano, nu_mes, ds_dose_vacina, nu_idade_paciente;"

# MenACWY: doses mensais por regiao/tipo de dose/idade
sqlite3 vacinacao.sqlite3 \
"CREATE VIEW IF NOT EXISTS
	menacwy_ano_mes_regiao_dose_idade (no_regiao, nu_ano, nu_mes, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) AS
	SELECT no_regiao, strftime('%Y', dt_vacina) as nu_ano, strftime('%m', dt_vacina) as nu_mes, sg_vacina, 
	       ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas
	FROM vacinacao, regiao_uf
	WHERE sg_uf=sg_uf_paciente
	AND nu_idade_paciente>=11
	AND nu_idade_paciente<=14
	AND sg_vacina='MenACWY'
	AND (ds_dose_vacina='Dose' OR ds_dose_vacina='Reforço' OR ds_dose_vacina='Única')
	GROUP BY no_regiao, nu_ano, nu_mes, ds_dose_vacina, nu_idade_paciente;"

# HPV4: doses mensais por regiao/tipo de dose/idade
sqlite3 vacinacao.sqlite3 \
"CREATE VIEW IF NOT EXISTS 
	hpv4_ano_mes_regiao_dose_idade (no_regiao, nu_ano, nu_mes, sg_vacina, ds_dose_vacina, nu_idade_paciente, nu_doses_aplicadas) AS
	SELECT no_regiao, strftime('%Y', dt_vacina) as nu_ano, strftime('%m', dt_vacina) as nu_mes, sg_vacina, 
	ds_dose_vacina, nu_idade_paciente, count(*) as doses_aplicadas
	FROM vacinacao, regiao_uf
	WHERE sg_uf=sg_uf_paciente
	AND nu_idade_paciente>=9
	AND nu_idade_paciente<=14
	AND sg_vacina='HPV4'
	AND (ds_dose_vacina='1ª Dose' OR ds_dose_vacina='2ª Dose' OR ds_dose_vacina='Única')
	GROUP BY no_regiao, nu_ano, nu_mes, ds_dose_vacina, nu_idade_paciente;"

# cobertura vacinal mensal de adolescentes por regiao/tipo de dose/faixa-etaria
sqlite3 vacinacao.sqlite3 \
"CREATE VIEW IF NOT EXISTS 
	cobertura_regiao_adolescentes_ano_mes (nu_ano, nu_mes, sg_vacina, ds_dose_vacina, ds_faixa_etaria,
					       no_regiao, nu_doses_aplicadas, nu_populacao, nu_cobertura_vacinal) AS
	SELECT nu_ano, nu_mes, sg_vacina, ds_dose_vacina, format('%u-%u',min(nu_idade_paciente),max(nu_idade_paciente)), 
		v.no_regiao, sum(nu_doses_aplicadas) as nu_doses_aplicadas, sum(nu_populacao),
		(100.0 * sum(nu_doses_aplicadas) / sum(nu_populacao)) 
	FROM censo2022regiao_idade c, dengue_ano_mes_regiao_dose_idade v
	WHERE c.no_regiao=v.no_regiao
	AND v.nu_idade_paciente=(c.nu_idade+nu_ano-2022)
	GROUP BY nu_ano, nu_mes, c.no_regiao, v.ds_dose_vacina
	UNION
	SELECT nu_ano, nu_mes, sg_vacina, ds_dose_vacina, format('%u-%u',min(nu_idade_paciente),max(nu_idade_paciente)),
		v.no_regiao, sum(nu_doses_aplicadas) as nu_doses_aplicadas, sum(nu_populacao),
		(100.0 * sum(nu_doses_aplicadas) / sum(nu_populacao))
	FROM censo2022regiao_idade c, menacwy_ano_mes_regiao_dose_idade v 
	WHERE c.no_regiao=v.no_regiao 
	AND v.nu_idade_paciente=(c.nu_idade+nu_ano-2022)
	GROUP BY nu_ano, nu_mes, c.no_regiao, v.ds_dose_vacina
	UNION
	SELECT nu_ano, nu_mes, sg_vacina, ds_dose_vacina, format('%u-%u',min(nu_idade_paciente),max(nu_idade_paciente)),
		v.no_regiao, sum(nu_doses_aplicadas) as nu_doses_aplicadas, sum(nu_populacao), 
		(100.0 * sum(nu_doses_aplicadas) / sum(nu_populacao))
	FROM censo2022regiao_idade c, hpv4_ano_mes_regiao_dose_idade v
	WHERE c.no_regiao=v.no_regiao
	AND v.nu_idade_paciente=(c.nu_idade+nu_ano-2022)
	GROUP BY nu_ano, nu_mes, c.no_regiao, v.ds_dose_vacina
	ORDER BY nu_ano, sg_vacina, v.no_regiao, nu_mes, nu_doses_aplicadas DESC;"


# Tabela Regioes/UF
sqlite3 vacinacao.sqlite3 "DROP TABLE IF EXISTS regiao_uf;"
sqlite3 vacinacao.sqlite3 "CREATE TABLE regiao_uf ( \"no_regiao\" STRING, \"sg_uf\" STRING, \"no_uf\" STRING, \"co_uf\" INTEGER);"
echo "Centro-Oeste,DF,Distrito Federal,53
Centro-Oeste,GO,Goiás,52
Centro-Oeste,MS,Mato Grosso do Sul,50
Centro-Oeste,MT,Mato Grosso,51
Nordeste,AL,Alagoas,27
Nordeste,BA,Bahia,29
Nordeste,CE,Ceará,23
Nordeste,MA,Maranhão,21
Nordeste,PB,Paraíba,25
Nordeste,PE,Pernambuco,26
Nordeste,PI,Piauí,22
Nordeste,RN,Rio Grande do Norte,24
Nordeste,SE,Sergipe,28
Norte,AC,Acre,12
Norte,AP,Amapá,16
Norte,AM,Amazonas,13
Norte,PA,Pará,15
Norte,RO,Rondônia,11
Norte,RR,Rorraima,14
Norte,TO,Tocantins,17
Sudeste,ES,Espírito Santo,32
Sudeste,MG,Minas Gerais,31
Sudeste,RJ,Rio de Janeiro,33
Sudeste,SP,São Paulo,35
Sul,PR,Paraná,41
Sul,RS,Rio Grande do Sul,43
Sul,SC,Santa Catarina,42" | sqlite3 --csv vacinacao.sqlite3 ".import '|cat -' regiao_uf"


