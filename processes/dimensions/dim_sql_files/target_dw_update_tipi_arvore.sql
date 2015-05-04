update dwao.dim_tipi_arvore
    set
        id = %s
        , nome = %s
        , state = %s
        , grupos = %s
        , parent = %s
    where
        iddim_tipi_arvore = %s
        , id = %s