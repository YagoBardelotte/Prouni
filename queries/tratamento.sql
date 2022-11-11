UPDATE tb_completa  
SET TIPO_BOLSA = CASE
                    WHEN TIPO_BOLSA = 'BOLSA INTEGRAL' THEN 'INTEGRAL'
                    WHEN TIPO_BOLSA = 'BOLSA PARCIAL 50%' THEN 'PARCIAL 50%'
                    WHEN TIPO_BOLSA = 'BOLSA COMPLEMENTAR 25%' THEN 'COMPL 25%'
                    WHEN TIPO_BOLSA = 'PARCIAL' THEN 'PARCIAL 50%'
                    ELSE UPPER(TIPO_BOLSA)
                END,
    MODALIDADE_ENSINO_BOLSA = CASE
                                 WHEN MODALIDADE_ENSINO_BOLSA = 'Presencial' THEN 'PRESENCIAL'
                                 WHEN MODALIDADE_ENSINO_BOLSA = 'EDUCAÇÃO A DISTÂNCIA' THEN 'EAD'
                                 ELSE UPPER(MODALIDADE_ENSINO_BOLSA)
                              END,
    NOME_TURNO_CURSO_BOLSA = CASE
                                WHEN NOME_TURNO_CURSO_BOLSA = 'CURSO A DISTÂNCIA' THEN 'EAD'
                                WHEN NOME_TURNO_CURSO_BOLSA = 'Curso a distância' THEN 'EAD'
                                ELSE UPPER(NOME_TURNO_CURSO_BOLSA)
                              END,
    SEXO_BENEFICIARIO_BOLSA = CASE
                                 WHEN SEXO_BENEFICIARIO_BOLSA = 'Feminino' THEN 'F'
                                 WHEN SEXO_BENEFICIARIO_BOLSA = 'Masculino' THEN 'M'
                              END,
    RACA_BENEFICIARIO_BOLSA = CASE
                                 WHEN RACA_BENEFICIARIO_BOLSA = 'Ind¡gena' THEN 'Indígena'
                                 ELSE RACA_BENEFICIARIO_BOLSA
                              END,
    BENEFICIARIO_DEFICIENTE_FISICO = CASE
                                        WHEN BENEFICIARIO_DEFICIENTE_FISICO = 'NÃO' THEN 'N'
                                        WHEN BENEFICIARIO_DEFICIENTE_FISICO = 'SIM' THEN 'S'
                                     END,
    REGIAO_BENEFICIARIO_BOLSA = UPPER(REGIAO_BENEFICIARIO_BOLSA);