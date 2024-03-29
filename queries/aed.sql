SELECT {info}
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2005) {info}_2005
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2006) {info}_2006
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2007) {info}_2007
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2008) {info}_2008
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2009) {info}_2009
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2010) {info}_2010
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2011) {info}_2011
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2012) {info}_2012
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2013) {info}_2013
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2014) {info}_2014
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2015) {info}_2015
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2016) {info}_2016
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2017) {info}_2017
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2018) {info}_2018
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2019) {info}_2019
     , COUNT({info}) FILTER (WHERE ANO_CONCESSAO_BOLSA =  2020) {info}_2020
FROM tb_completa
WHERE {info} IS NOT NULL
GROUP BY {info}
ORDER BY {info}_2005 DESC;