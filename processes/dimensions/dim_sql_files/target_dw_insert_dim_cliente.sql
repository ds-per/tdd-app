insert into
    dim_cliente (
        codigocliente
        , nome
        , provincia
        , municipio
        , sexo
        , data_nascimento
        , estado_civil
        , telefone
        , telemovel
        , email
        , version
        , hash
        , source
        )
        values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)