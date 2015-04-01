update	dim_cliente
    set
        nome = %s
        , provincia = %s
        , municipio = %s
        , sexo = %s
        , data_nascimento = %s
        , estado_civil = %s
        , telefone = %s
        , telemovel = %s
        , email = %s
        , version = %s
        , hash = %s
        , source = %s
    where
        codigocliente = %s and source = %s