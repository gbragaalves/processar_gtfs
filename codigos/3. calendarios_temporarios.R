# Este codigo ajusta os calendarios do GTFS em virtude de interdições, eventos
# e demais mudanças de itinerário de médio e longo prazo.

pacman::p_load(gtfstools, dplyr, data.table, googlesheets4, purrr, tidyverse, Hmisc)

ano_gtfs <- "2023"
mes_gtfs <- "07"
quinzena_gtfs <- "01"

endereco_gtfs <- file.path(
  "../../dados/gtfs", ano_gtfs,
  paste0("sppo_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip")
)

gtfs <- read_gtfs(endereco_gtfs)

desvios_tabela_id <- "1L7Oq1vqG5S_uOs_NdqgF4HG-Ac6gEyZrzQJYLpZH3OI"

tabela_desvios <- read_sheet(desvios_tabela_id, sheet = "linhas_desvios")
descricao_desvios <- read_sheet(desvios_tabela_id, sheet = "descricao_desvios") %>%
  filter(data_inicio < Sys.Date(), data_fim > Sys.Date())

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
    service_id %in% c("U", "S", "D") & !(route_id %in% rotas_afetadas_desvios) ~ paste0(service_id, "_REG"),
    TRUE ~ service_id
  ))

gtfs$calendar <- gtfs$calendar %>%
  mutate(service_id = recode(service_id, U = "U_REG", S = "S_REG", D = "D_REG"))

lista_eventos <- descricao_desvios$cod_desvio %>%
  unique() %>%
  na.omit()

excecoes <- data.frame()

for (evento in lista_eventos) {
  linhas_afetadas_evento <- tabela_desvios %>%
    filter(!!sym(evento)) %>%
    select(servico) %>%
    left_join(select(gtfs$routes, route_short_name, route_id), by = c("servico" = "route_short_name")) %>%
    pull(route_id)

  gtfs$trips <- gtfs$trips %>%
    mutate(service_id = case_when(
      service_id %in% c("U", "S", "D") & route_id %in% linhas_afetadas_evento ~ paste0(service_id, "_DESAT_", evento),
      TRUE ~ service_id
    ))

  calendarios_novos <- gtfs$calendar %>%
    filter(service_id %in% c(paste0(c("U", "S", "D"), "_", evento))) %>%
    mutate(service_id = case_when(
      service_id %in% paste0(c("U", "S", "D"), "_", evento) ~ paste0(service_id, "_DESAT_", evento),
      TRUE ~ service_id
    ))

  gtfs$calendar <- bind_rows(gtfs$calendar, calendarios_novos)

  tabela_evento <- descricao_desvios %>% filter(cod_desvio == evento)

  excecoes_datas_ativar <- gtfs$calendar_dates %>%
    filter(as_date(date) %in% seq(as_date(tabela_evento$data_inicio), as_date(tabela_evento$data_fim), by = "day")) %>%
    mutate(service_id = case_when(
      service_id %in% c("U", "S", "D") ~ paste0(service_id, "_", evento),
      TRUE ~ service_id
    ))

  excecoes_datas_desativar <- gtfs$calendar_dates %>%
    filter(as_date(date) %in% seq(as_date(tabela_evento$data_inicio), as_date(tabela_evento$data_fim), by = "day")) %>%
    mutate(service_id = case_when(
      service_id %in% c("U", "S", "D") ~ paste0(service_id, "_DESAT_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "2")

  calendar_dates_incluir <- tabela_evento %>%
    mutate(
      data_inicio = as_date(data_inicio),
      data_fim = as_date(data_fim)
    ) %>%
    rowwise() %>%
    mutate(date = list(seq(data_inicio, data_fim, by = "day"))) %>%
    tidyr::unnest(date) %>%
    select(service_id = cod_desvio, date) %>%
    filter(!date %in% excecoes_datas_ativar$date) %>%
    mutate(service_id = case_when(
      lubridate::wday(date) %in% c(2:6) ~ paste0("U_", evento),
      lubridate::wday(date) == 7 ~ paste0("S_", evento),
      lubridate::wday(date) == 1 ~ paste0("D_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "1")

  calendar_dates_excluir <- tabela_evento %>%
    mutate(
      data_inicio = as_date(data_inicio),
      data_fim = as_date(data_fim)
    ) %>%
    rowwise() %>%
    mutate(date = list(seq(data_inicio, data_fim, by = "day"))) %>%
    tidyr::unnest(date) %>%
    select(service_id = cod_desvio, date) %>%
    filter(!date %in% excecoes_datas_desativar$date) %>%
    mutate(service_id = case_when(
      lubridate::wday(date) %in% c(2:6) ~ paste0("U_DESAT_", evento),
      lubridate::wday(date) == 7 ~ paste0("S_DESAT_", evento),
      lubridate::wday(date) == 1 ~ paste0("D_DESAT_", evento),
      TRUE ~ service_id
    )) %>%
    mutate(exception_type = "2")

  calendar_dates_evento <- rbindlist(list(
    calendar_dates_incluir,
    calendar_dates_excluir,
    excecoes_datas_ativar,
    excecoes_datas_desativar
  )) %>%
    arrange(date)

  excecoes <- excecoes %>%
    bind_rows(calendar_dates_evento)
}

gtfs$calendar_dates <- gtfs$calendar_dates %>%
  mutate(
    service_id = case_when(
      service_id %in% c("U", "S", "D") ~ paste0(service_id, "_REG"),
      TRUE ~ service_id
    ),
    exception_type = as.character(exception_type)
  ) %>%
  bind_rows(excecoes)

feed_info <- gtfs$feed_info %>%
  mutate(
    feed_start_date = lubridate::ymd(feed_start_date),
    feed_end_date = lubridate::ymd(feed_end_date)
  )

gtfs$calendar_dates <- gtfs$calendar_dates %>%
  filter(date %in% seq(ymd(gtfs$feed_info$feed_start_date),
    ymd(gtfs$feed_info$feed_end_date) + 60,
    by = "day"
  ))

write_gtfs(gtfs, endereco_gtfs)
