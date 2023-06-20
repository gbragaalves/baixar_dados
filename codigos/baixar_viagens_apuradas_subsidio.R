### Este código é utilizado para baixar os registros das viagens de ônibus
### apuradas pela equipe de monitoramento da SMTR. Os dados destas viagens
### servem de insumo para a maioria dos produtos gerados no R. Antes de
### executar qualquer análise ou processo é importante assegurar que os
### dados de viagens do período correspondente foram baixados.

pacman::p_load(lubridate, glue, basedosdados, stringr)

### Define a data inicial para download de dados de viagens. Há dados
### disponíveis desde 01 de junho de 2022.

dia_inicio <- as_date("2023-06-01")

### Define se será feito o download de dados para um período específico ou
### se serão baixados todos os dados disponíveis desde o dia_inicio até a data
### de execução do código. No caso de execução de análise de dados históricos
### é recomendado definir uma data limite para download dos dados.

definir_dia_fim <- FALSE

if (definir_dia_fim) {
  dia_fim <- as_date("2022-06-30")
} else {
  dia_fim <- Sys.Date() - 1
}

basedosdados::set_billing_id("rj-smtr")

dia <- dia_inicio

for (dia in seq(dia_inicio, dia_fim, by = "day")) {
  
  dia <- as_date(dia)
  
  print(dia)

  end_trip <-
    file.path(
      "../../dados/viagens/sppo", year(dia),
      str_pad(month(dia), width = 2, pad = "0"), glue("{dia}.rds")
    )

  if (file.exists(end_trip)) {
    next
  } else {
    trip_dia <-
      glue("SELECT * FROM `rj-smtr.projeto_subsidio_sppo.viagem_completa` where data = '{dia}'")

    trip <- basedosdados::read_sql(trip_dia)
    if (nrow(trip) > 2) {
      pasta_mes <- file.path(
        "../../dados/viagens/sppo", year(dia),
        str_pad(month(dia), width = 2, pad = "0")
      )
      if (!dir.exists(pasta_mes)) {
        dir.create(pasta_mes, recursive = TRUE)
      }
      saveRDS(trip, end_trip)
    }
    rm(trip)
  }
}
