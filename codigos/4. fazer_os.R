pacman::p_load(gtfstools, dplyr, data.table,sf,lubridate,gt,stringr)

ano_gtfs <- "2023"
mes_gtfs <- "08"
quinzena_gtfs <- "01"

endereco_gtfs <- file.path(
  "../../dados/gtfs", ano_gtfs,
  paste0("sppo_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip")
)

gtfs <- read_gtfs(endereco_gtfs)

frescoes <- gtfs$routes %>% 
  filter(route_type == '200') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,frescoes,keep = F)

trips_fantasma <- fread("../../dados/insumos/trip_id_fantasma.txt") %>%
  unlist()

trips_desat <- gtfs$trips %>% 
  filter(service_id %like% 'DESAT') %>% 
  pull(trip_id)

gtfs <- gtfs %>% 
  filter_by_trip_id(trips_desat, keep = F) %>% 
  filter_by_trip_id(trips_fantasma, keep = F)

gtfs$shapes <- as.data.table(gtfs$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes_sf <- convert_shapes_to_sf(gtfs) %>% 
  st_transform(31983) %>% 
  mutate(extensao = as.integer(st_length(.))) %>% 
  st_drop_geometry()

viagens_freq <- gtfs$frequencies %>%
  mutate(start_time = if_else(lubridate::hour(hms(start_time)) >= 24,
                              hms(start_time) - lubridate::hours(24),
                              hms(start_time)
  )) %>%
  mutate(end_time = if_else(lubridate::hour(hms(end_time)) >= 24,
                            hms(end_time) - lubridate::hours(24),
                            hms(end_time)
  )) %>%
  mutate(
    start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
                       sprintf("%02d", lubridate::minute(start_time)),
                       sprintf("%02d", lubridate::second(start_time)),
                       sep = ":"
    ),
    end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
                     sprintf("%02d", lubridate::minute(end_time)),
                     sprintf("%02d", lubridate::second(end_time)),
                     sep = ":"
    )
  ) %>%
  mutate(
    start_time = as.POSIXct(start_time,
                            format = "%H:%M:%S", origin =
                              "1970-01-01"
    ),
    end_time = as.POSIXct(end_time,
                          format = "%H:%M:%S", origin =
                            "1970-01-01"
    )
  ) %>%
  mutate(start_time = if_else(as.ITime(start_time) < as.ITime("02:00:00"),
                              start_time + 86400,
                              start_time
  )) %>%
  mutate(end_time = if_else(end_time < start_time,
                            end_time + 86400,
                            end_time
  )) %>%
  mutate(
    duracao = as.numeric(difftime(end_time, start_time, units = "secs")),
    partidas = as.numeric(duracao / headway_secs)
  ) %>% 
  left_join(select(gtfs$trips,trip_id,trip_short_name,trip_headsign,direction_id,service_id)) %>% 
  filter(!(service_id %like% 'DESAT')) %>% 
  mutate(circular = if_else(nchar(trip_headsign)==0,T,F)) %>% 
  mutate(tipo_dia = substr(service_id,1,1))


os_stop_times <- gtfs$stop_times %>% 
  filter(!(trip_id %in% viagens_freq$trip_id)) %>% 
  left_join(select(gtfs$trips,trip_id,trip_short_name,trip_headsign,direction_id,service_id)) %>% 
  filter(!(service_id %like% 'DESAT')) %>% 
  mutate(circular = if_else(nchar(trip_headsign)==0,T,F)) %>% 
  mutate(tipo_dia = substr(service_id,1,1)) %>% 
  filter(stop_sequence == '0') %>% 
  group_by(trip_short_name,direction_id,tipo_dia) %>% 
  arrange(departure_time) %>% 
  mutate(headway_secs = as.integer(difftime(as.ITime(departure_time),as.ITime(lag(departure_time)),units = 'secs'))) %>% 
  summarise(partidas = n(),
            inicio = min(departure_time),
            fim = max(departure_time),
            pico = min(headway_secs, na.rm = T),
            circular = unique(circular)) %>% 
  rename(servico = trip_short_name) %>% 
  ungroup() %>% 
  mutate(pico = if_else(is.finite(pico),pico,0)) %>% 
  mutate(inicio = as.POSIXct(as.ITime(inicio)),
         fim = as.POSIXct(as.ITime(fim)))  %>% 
  mutate(inicio = lubridate::force_tz(inicio, "America/Sao_Paulo"),
         fim = lubridate::force_tz(fim, "America/Sao_Paulo"))

trips_manter <- gtfs$trips %>%
  mutate(
    letras = stringr::str_extract(trip_short_name, "[A-Z]+"),
    numero = stringr::str_extract(trip_short_name, "[0-9]+")
  ) %>%
  tidyr::unite(., trip_short_name, letras, numero, na.rm = T, sep = "") %>%
  left_join(select(viagens_freq, trip_id, partidas)) %>%
  mutate(partidas = if_else(is.na(partidas), 1, partidas)) %>%
  group_by(shape_id) %>%
  mutate(ocorrencias = sum(partidas)) %>%
  ungroup() %>%
  group_by(route_id, direction_id) %>%
  slice_max(ocorrencias, n = 1) %>%
  ungroup() %>%
  distinct(shape_id, trip_short_name, .keep_all = T) %>%
  select(trip_id, trip_short_name, shape_id, direction_id) %>% 
  left_join(select(shapes_sf,shape_id,extensao)) %>% 
  group_by(trip_short_name, direction_id) %>% 
  slice_min(extensao, n = 1)

a <- trips_manter %>% 
  group_by(trip_short_name) %>% 
  summarise(qt = n())

os_freq <- viagens_freq %>% 
  group_by(trip_short_name,direction_id,tipo_dia) %>% 
  summarise(partidas = sum(partidas),
            inicio = min(start_time),
            fim = max(end_time),
            pico = min(headway_secs),
            circular = unique(circular)) %>% 
  rename(servico = trip_short_name) %>% 
  ungroup() %>% 
  mutate(inicio = lubridate::force_tz(inicio, "America/Sao_Paulo"),
                   fim = lubridate::force_tz(fim, "America/Sao_Paulo"))

os_bruto <- bind_rows(os_freq,os_stop_times)

pico_du <- os_bruto %>% 
  filter(tipo_dia == 'U') %>% 
  group_by(servico) %>% 
  summarise(pico_du = min(pico)) %>% 
  mutate(pico_du = pico_du/60)

linhas_partidas_problemas <- os_bruto %>% 
  filter(partidas%%1!=0)

os <- os_bruto %>% 
  ungroup() %>% 
  distinct(servico,circular) %>% 
  arrange(servico) %>% 
  left_join(select(gtfs$routes,route_short_name,route_long_name,agency_id), by = c('servico'='route_short_name')) %>% 
  left_join(select(gtfs$agency,agency_id,agency_name)) %>% 
  select(-c(agency_id)) %>% 
  left_join(select(trips_manter,trip_short_name,extensao,direction_id) %>% filter(direction_id=='0'), by = c('servico'='trip_short_name')) %>% 
  select(-c(direction_id)) %>% 
  rename(extensao_ida = extensao) %>% 
  left_join(select(trips_manter,trip_short_name,extensao,direction_id) %>% filter(direction_id=='1'), by = c('servico'='trip_short_name')) %>% 
  select(-c(direction_id)) %>% 
  rename(extensao_volta = extensao)

du <- os_bruto %>% 
  filter(tipo_dia == 'U') %>% 
  group_by(servico) %>% 
  summarise(partidas_ida_du = sum(partidas[direction_id=='0']),
            partidas_volta_du = sum(partidas[direction_id=='1']),
            inicio_du = min(inicio),
            fim_du = max(fim),
            pico_du = min(pico)/60,
            circular = unique(circular)) %>% 
  mutate(viagens_du = if_else(circular,
                              partidas_ida_du+partidas_volta_du,
                              (partidas_ida_du+partidas_volta_du)/2)) %>% 
  select(-c(circular))

sab <- os_bruto %>% 
  filter(tipo_dia == 'S') %>% 
  group_by(servico) %>% 
  summarise(partidas_ida_sab = sum(partidas[direction_id=='0']),
            partidas_volta_sab = sum(partidas[direction_id=='1']),
            inicio_sab = min(inicio),
            fim_sab = max(fim),
            pico_sab = min(pico)/60,
            circular = unique(circular)) %>% 
  mutate(viagens_sab = if_else(circular,
                              partidas_ida_sab+partidas_volta_sab,
                              (partidas_ida_sab+partidas_volta_sab)/2)) %>% 
  select(-c(circular))

dom <- os_bruto %>% 
  filter(tipo_dia == 'D') %>% 
  group_by(servico) %>% 
  summarise(partidas_ida_dom = sum(partidas[direction_id=='0']),
            partidas_volta_dom = sum(partidas[direction_id=='1']),
            inicio_dom = min(inicio),
            fim_dom = max(fim),
            pico_dom = min(pico)/60,
            circular = unique(circular)) %>% 
  mutate(viagens_dom = if_else(circular,
                              partidas_ida_dom+partidas_volta_dom,
                              (partidas_ida_dom+partidas_volta_dom)/2)) %>% 
  select(-c(circular))

substituir_na_por_traco <- function(x) {
  str_replace_all(x, "NA:NA:NA", "—")
}

os_final <- os %>% 
  left_join(du) %>% 
  left_join(sab) %>% 
  left_join(dom) %>% 
  mutate(extensao_ida = if_else(is.na(extensao_ida),0,extensao_ida),
         extensao_volta = if_else(is.na(extensao_volta),0,extensao_volta)) %>% 
  mutate(km_du = round(((partidas_ida_du*extensao_ida)+(partidas_volta_du*extensao_volta))/1000,2),
         km_sab = round(((partidas_ida_sab*extensao_ida)+(partidas_volta_sab*extensao_volta))/1000,2),
         km_dom = round(((partidas_ida_dom*extensao_ida)+(partidas_volta_dom*extensao_volta))/1000,2)) %>% 
  mutate(partidas_ida_pf = ceiling(partidas_ida_du*0.625),
         partidas_volta_pf = ceiling(partidas_volta_du*0.625),
         viagens_pf = if_else(circular,
                              partidas_ida_pf+partidas_volta_pf,
                               (partidas_ida_pf+partidas_volta_pf)/2),
         km_pf = round(((partidas_ida_pf*extensao_ida)+(partidas_volta_pf*extensao_volta))/1000,2)) %>% 
  select(servico,route_long_name,agency_name,extensao_ida,extensao_volta,
         inicio_du,fim_du,pico_du,partidas_ida_du,partidas_volta_du,viagens_du,km_du,
         inicio_sab,fim_sab,pico_sab,partidas_ida_sab,partidas_volta_sab,viagens_sab,km_sab,
         inicio_dom,fim_dom,pico_dom,partidas_ida_dom,partidas_volta_dom,viagens_dom,km_dom,
         partidas_ida_pf,partidas_volta_pf,viagens_pf,km_pf) %>%
  mutate(fim_du = paste(sprintf("%02d", if_else(lubridate::day(fim_du)>lubridate::day(inicio_du),
                                               as.integer(lubridate::hour(fim_du))+24,
                                               lubridate::hour(fim_du))), 
                        sprintf("%02d", lubridate::minute(fim_du)),
                        sprintf("%02d", lubridate::second(fim_du)),sep=':'),
         inicio_du = paste(sprintf("%02d", lubridate::hour(inicio_du)),
                            sprintf("%02d", lubridate::minute(inicio_du)),
                            sprintf("%02d", lubridate::second(inicio_du)),sep=':')) %>% 
  
  mutate(fim_sab = paste(sprintf("%02d", if_else(lubridate::day(fim_sab)>lubridate::day(inicio_sab),
                                                as.integer(lubridate::hour(fim_sab))+24,
                                                lubridate::hour(fim_sab))), 
                        sprintf("%02d", lubridate::minute(fim_sab)),
                        sprintf("%02d", lubridate::second(fim_sab)),sep=':'),
         inicio_sab = paste(sprintf("%02d", lubridate::hour(inicio_sab)),
                           sprintf("%02d", lubridate::minute(inicio_sab)),
                           sprintf("%02d", lubridate::second(inicio_sab)),sep=':')) %>% 
  
  mutate(fim_dom = paste(sprintf("%02d", if_else(lubridate::day(fim_dom)>lubridate::day(inicio_dom),
                                                as.integer(lubridate::hour(fim_dom))+24,
                                                lubridate::hour(fim_dom))), 
                        sprintf("%02d", lubridate::minute(fim_dom)),
                        sprintf("%02d", lubridate::second(fim_dom)),sep=':'),
         inicio_dom = paste(sprintf("%02d", lubridate::hour(inicio_dom)),
                           sprintf("%02d", lubridate::minute(inicio_dom)),
                           sprintf("%02d", lubridate::second(inicio_dom)),sep=':')) %>% 
  
  mutate(pico_du = as.character(ifelse(pico_du == 0,'—',pico_du)),
         pico_sab = as.character(ifelse(pico_sab == 0,'—',pico_sab)),
         pico_dom = as.character(ifelse(pico_dom == 0,'—',pico_dom))) %>% 
  mutate_all(substituir_na_por_traco) %>% 
  mutate_all(~if_else(is.na(.), "—", .)) %>% 
  rename(vista = route_long_name,
         consorcio = agency_name) %>% 
  mutate_all(~gsub("\\.", ",", .))

nome_colunas <- c('Serviço','Vista','Consórcio','Extensão de Ida','Extensão de Volta',
                  'Horário Inicial Dia Útil','Horário Fim Dia Útil','Intervalo de Pico Dia Útil',
                  'Partidas Ida Dia Útil',	'Partidas Volta Dia Útil',	'Viagens Dia Útil',	'Quilometragem Dia Útil',
                  'Horário Inicial Sábado','Horário Fim Sábado','Intervalo de Pico Sábado',
                  'Partidas Ida Sábado',	'Partidas Volta Sábado',	'Viagens Sábado',	'Quilometragem Sábado',
                  'Horário Inicial Domingo','Horário Fim Domingo','Intervalo de Pico Domingo',
                  'Partidas Ida Domingo',	'Partidas Volta Domingo',	'Viagens Domingo',	'Quilometragem Domingo',
                  'Partidas Ida Ponto Facultativo',	'Partidas Volta Ponto Facultativo',	'Viagens Ponto Facultativo',	'Quilometragem Ponto Facultativo')

colnames(os_final) <- nome_colunas

fwrite(os_final,"D:/temp/os.csv",sep=';',dec=',')

os_pdf <- os_final %>% 
  select('Serviço','Vista','Consórcio','Extensão de Ida','Extensão de Volta',
         'Horário Inicial Dia Útil','Horário Fim Dia Útil',
         'Partidas Ida Dia Útil',	'Partidas Volta Dia Útil',	'Viagens Dia Útil',	'Quilometragem Dia Útil',
         'Partidas Ida Sábado',	'Partidas Volta Sábado',	'Viagens Sábado',	'Quilometragem Sábado',
         'Partidas Ida Domingo',	'Partidas Volta Domingo',	'Viagens Domingo',	'Quilometragem Domingo',
         'Partidas Ida Ponto Facultativo',	'Partidas Volta Ponto Facultativo',	'Viagens Ponto Facultativo',	'Quilometragem Ponto Facultativo') %>% 
  rename('Horário de Início' = 'Horário Inicial Dia Útil',
         'Horário de Fim' = 'Horário Fim Dia Útil')

gt(os_pdf) %>% 
  gtsave('D:/temp/os.pdf')
         