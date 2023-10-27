rename.brunetti.T <- function(tvar) {
    switch(tvar,
        T_MAX = "TX",
        T_MIN = "TN",
        stop("Invalid variable name: ", tvar)
    )
}

brunetti.anagrafica_file.relative <- function(db, tvar, flavor) {
    switch(flavor,
        raw = switch(db,
            DPC = "ANAGRAFICA",
            BRUN = "ANAGRAFICA_TOT",
            stop("Invalid database name: ", db)
        ),
        qc_era5 = "ANAGRAFICA",
        qc_homo = "ANAGRAFICA_OK"
    )
}

brunetti.root.relative <- function(db, tvar, flavor) {
    tvar <- rename.brunetti.T(tvar)
    file.path(
        "DPC",
        switch(flavor,
            raw = "01_DATI_SINTESI_",
            qc_era5 = "03_STEP02_TH05_QCSYNT_DATA_DD_DPC_",
            qc_homo = "04_QC_DATA_DD_DPC_",
            stop("Invalid section name: ", flavor)
        ) |> paste0(tvar)
    )
}
