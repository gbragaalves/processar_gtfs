# Este codigo ajusta os calendarios do GTFS em virtude de interdições, eventos
# e demais mudanças de itinerário de médio e longo prazo.

pacman::p_load(gtfstools, dplyr, data.table, googlesheets4, purrr, tidyverse, Hmisc)

ano_gtfs <- "2023"
mes_gtfs <- "06"
quinzena_gtfs <- "02"

endereco_gtfs <- paste0(
  "../../dados/gtfs/", ano_gtfs, "/sppo",
  "_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip"
)

gtfs <- read_gtfs(endereco_gtfs)

desvios_tabela_id <- "1L7Oq1vqG5S_uOs_NdqgF4HG-Ac6gEyZrzQJYLpZH3OI"

tabela_desvios <- read_sheet(desvios_tabela_id, "linhas_desvios")
descricao_desvios <- read_sheet(desvios_tabela_id, "descricao_desvios") %>%
  filter(data_inicio < Sys.Date()) %>%
  filter(data_fim > Sys.Date())

desvios <- c("servico", descricao_desvios$cod_desvio)

tabela_desvios <- tabela_desvios %>%
  select(all_of(desvios))

linhas_afetadas_desvios <- tabela_desvios %>%
  filter(rowSums(select_if(., is.logical)) > 0) %>%
  pull(servico)

rotas_afetadas_desvios <- gtfs$routes %>%
  filter(route_short_name %in% linhas_afetadas_desvios) %>%
  pull(route_id)

gtfs$trips <- gtfs$trips %>%
  mutate(service_id = case_when(
    service_id == "U" & !(route_id %in% rotas_afetadas_desvios) ~ "U_REG",
    service_id == "S" & !(route_id %in% rotas_afetadas_desvios) ~ "S_REG",
    service_id == "D" & !(route_id %in% rotas_afetadas_desvios) ~ "D_REG",
    TRUE ~ service_id
  ))


gtfs$calendar <- gtfs$calendar %>%
  mutate(service_id = recode(service_id, U = "U_REG", S = "S_REG", D = "D_REG"))

lista_eventos <- unique(descricao_desvios$cod_desvio) %>%
  na.omit()

excecoes <- data.frame()

for (i in 1:length(lista_eventos)) {
  evento <- lista_eventos[i]

  linhas_afetadas_evento <- tabela_desvios %>%
    filter(get(evento)) %>%
    select(servico) %>%
    left_join(select(gtfs$routes, route_short_name, route_id), by = c("servico" = "route_short_name")) %>%
    pull(route_id)

  gtfs$trips <- gtfs$trips %>%
    mutate(service_id = case_when(
      service_id == "U" & route_id %in% linhas_afetadas_evento ~ paste0("U_DESAT_", evento),
      service_id == "S" & route_id %in% linhas_afetadas_evento ~ paste0("S_DESAT_", evento),
      service_id == "D" & route_id %in% linhas_afetadas_evento ~ paste0("D_DESAT_", evento),
      TRUE ~ service_id
    ))

  calendarios_novos <- gtfs$calendar %>%
    filter(service_id %in% c(
      paste0("U_", evento),
      paste0("S_", evento),
      paste0("D_", evento)
    )) %>%
    mutate(service_id = case_when(
      service_id == paste0("U_", evento) ~ paste0("U_DESAT_", evento),
      service_id == paste0("S_", evento) ~ paste0("S_DESAT_", evento),
      service_id == paste0("D_", evento) ~ paste0("D_DESAT_", evento),
      TRUE ~ service_id
    ))

  gtfs$calendar <- gtfs$calendar %>%
    bind_rows(., calendarios_novos)

  tabela_evento <- descricao_desvios[i, ]

  excecoes_datas_at <- gtfs$calendar_dates %>%
    filter(as.IDate(date) %in% c(as.IDate(tabela_evento$data_inicio):as.IDate(tabela_evento$data_fim))) %>%
    mutate(service_id = case_when(
      service_id == "U" ~ paste0("U_", evento),
      service_id == "S" ~ paste0("S_", evento),
      service_id == "D" ~ paste0("D_", evento),
      TRUE ~ service_id
    ))

  excecoes_datas_desat <- gtfs$calendar_dates %>%
    filter(as.IDate(date) %in% c(as.IDate(tabela_evento$data_inicio):as.IDate(tabela_evento$data_fim))) %>%
    mutate(service_id = case_when(
      service_id == "U" ~ paste0("U_DESAT_", evento),
      service_id == "S" ~ paste0("S_DESAT_", evento),
      service_id == "D" ~ paste0("D_DESAT_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "2")

  calendar_dates_incluir <- tabela_evento %>%
    mutate(
      data_inicio = as.Date(data_inicio),
      data_fim = as.Date(data_fim)
    ) %>%
    rowwise() %>%
    mutate(date = list(seq(data_inicio, data_fim, by = "day"))) %>%
    tidyr::unnest(date) %>%
    select(service_id = cod_desvio, date) %>%
    filter(date %nin% excecoes_datas_at$date) %>%
    mutate(dia = lubridate::wday(date)) %>%
    mutate(service_id = case_when(
      dia %in% c(2:6) ~ paste0("U_", evento),
      dia == 7 ~ paste0("S_", evento),
      dia == 1 ~ paste0("D_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "1") %>%
    select(-c(dia))


  calendar_dates_excluir <- tabela_evento %>%
    mutate(
      data_inicio = as.Date(data_inicio),
      data_fim = as.Date(data_fim)
    ) %>%
    rowwise() %>%
    mutate(date = list(seq(data_inicio, data_fim, by = "day"))) %>%
    tidyr::unnest(date) %>%
    select(service_id = cod_desvio, date) %>%
    filter(date %nin% excecoes_datas_desat$date) %>%
    mutate(dia = lubridate::wday(date)) %>%
    mutate(service_id = case_when(
      dia %in% c(2:6) ~ paste0("U_DESAT_", evento),
      dia == 7 ~ paste0("S_DESAT_", evento),
      dia == 1 ~ paste0("D_DESAT_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "2") %>%
    select(-c(dia))

  calendar_dates_evento <- rbindlist(list(
    calendar_dates_incluir,
    calendar_dates_excluir,
    excecoes_datas_at,
    excecoes_datas_desat
  )) %>%
    arrange(date)

  excecoes <- excecoes %>%
    bind_rows(calendar_dates_evento)
}

gtfs$calendar_dates <- gtfs$calendar_dates %>%
  mutate(service_id = case_when(
    service_id == "U" ~ "U_REG",
    service_id == "S" ~ "S_REG",
    service_id == "D" ~ "D_REG",
    TRUE ~ service_id
  )) %>%
  mutate(exception_type = as.character(exception_type)) %>%
  bind_rows(excecoes)

feed_info <- gtfs$feed_info %>%
  mutate(
    feed_start_date = lubridate::ymd(feed_start_date),
    feed_end_date = lubridate::ymd(feed_end_date)
  )

gtfs$calendar_dates <- gtfs$calendar_dates %>%
  filter(as.IDate(date) %in% c(as.IDate(feed_info$feed_start_date):as.IDate(feed_info$feed_end_date)))

write_gtfs(gtfs, endereco_gtfs)
