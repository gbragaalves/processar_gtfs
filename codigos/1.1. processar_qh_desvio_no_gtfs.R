# Este codigo pega quadros horarios ja armazenados no GTFS e retorna o quadro
# de partidas, em formato pronto para importacao, apenas para as linhas e
# servicos definidas neste codigo.
#
# Util para reimportar frequencias quando outro calendario for aplicado.

pacman::p_load(gtfstools, dplyr, data.table, googlesheets4)

ano_gtfs <- "2023"
mes_gtfs <- "06"
quinzena_gtfs <- "02"

end_gtfs <- paste0(
  "../../dados/gtfs/", ano_gtfs, "/sppo_", ano_gtfs, "-",
  mes_gtfs, "-", quinzena_gtfs, "Q.zip"
)

gtfs <- read_gtfs(end_gtfs)

tabela_desvios <- read_sheet(
  "1L7Oq1vqG5S_uOs_NdqgF4HG-Ac6gEyZrzQJYLpZH3OI",
  "linhas_desvios"
)

linhas_desvios <-
  tabela_desvios %>%
  filter(rowSums(across(where(is.logical))) > 0) %>%
  select(servico) %>%
  unlist()

frequencias_desvios <- gtfs$frequencies %>%
  inner_join(select(gtfs$trips, trip_id, trip_short_name, trip_headsign, service_id), by = "trip_id") %>%
  filter(trip_short_name %in% linhas_desvios, service_id %in% c("U", "S", "D"))

linhas_processar <- frequencias_desvios %>%
  select(trip_short_name, trip_headsign, service_id) %>%
  distinct() %>%
  arrange(trip_short_name, desc(service_id))

servicos <- pull(linhas_processar, trip_short_name)
vistas <- pull(linhas_processar, trip_headsign)
calendarios <- pull(linhas_processar, service_id)
qt <- seq_along(servicos)


pasta_qh <- paste0("../../resultados/quadro_horario/", ano_gtfs, "/", mes_gtfs, "/qh_por_linha/")
purrr::possibly(dir.create)(pasta_qh, recursive = TRUE)


separarQuadros <- function(servicos, vistas, calendarios) {
  a <- frequencias_desvios %>%
    filter(trip_short_name == servicos) %>%
    filter(trip_headsign == vistas) %>%
    filter(service_id == calendarios) %>%
    arrange(start_time) %>%
    mutate(trip_id = "") %>%
    select(trip_id, trip_headsign, trip_short_name, start_time, end_time, headway_secs)

  nome_arq <- paste0("qh_", servicos, "-", vistas, "_", calendarios, ".csv")

  espaco <- data.frame(trip_id = "")

  quadro_final <- data.frame(calendarios) %>%
    mutate(calendarios = case_when(
      calendarios == "U" ~ "DIA UTIL",
      calendarios == "S" ~ "SABADO",
      calendarios == "D" ~ "DOMINGO",
      TRUE ~ calendarios
    ))

  quadro_final <- quadro_final %>%
    bind_rows(quadro_final) %>%
    bind_rows(quadro_final) %>%
    rename(trip_id = calendarios) %>%
    bind_rows(espaco) %>%
    bind_rows(espaco) %>%
    bind_rows(a)


  fwrite(a, paste0(pasta_qh, nome_arq), quote = F)
}

mapply(separarQuadros, servicos, vistas, calendarios)

