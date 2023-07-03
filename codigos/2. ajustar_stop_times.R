pacman::p_load(dplyr, gtfstools, lubridate, bizdays, data.table, tidyverse, tibble, tidyr)

ano_velocidade <- '2023'
mes_velocidade <- '05'

ano_gtfs <- '2023'
mes_gtfs <- '06'
quinzena_gtfs <- '02'

gtfs_processar <- 'brt'

endereco_gtfs <- paste0("../../dados/gtfs/",ano_gtfs,"/",gtfs_processar,
                        "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q.zip")

if (gtfs_processar == "brt") {
  path_nm <- paste0("../../dados/viagens/", gtfs_processar, "/", ano_velocidade, "/", mes_velocidade, "/validas")
  nm <- list.files(path = path_nm, full.names = TRUE, pattern = "*.csv", recursive = TRUE)
  trips <- map_df(nm, fread, colClasses = 'character') %>%
    select(servico, direction_id, datetime_partida, datetime_chegada, distancia_planejada, data) %>%
    mutate(distancia_planejada = as.numeric(distancia_planejada),
           distancia_planejada = distancia_planejada / 1000)
} else {
  path_nm <- paste0("../../dados/viagens/", gtfs_processar, "/", ano_velocidade, "/", mes_velocidade, "/")
  nm <- list.files(path = path_nm, full.names = TRUE, pattern = "*.rds", recursive = TRUE)
  trips <- map_df(nm, read_rds) %>%
    select(servico_informado, sentido, datetime_partida, datetime_chegada, distancia_planejada, data) %>%
    mutate(servico = servico_informado,
           direction_id = case_when(sentido == "I" ~ 0,
                                    sentido == "V" ~ 1,
                                    sentido == "C" ~ 0)) %>%
    select(servico, direction_id, datetime_partida, datetime_chegada, distancia_planejada, data)
  
  path_frescao <- paste0("../../dados/viagens/frescao/",ano_velocidade,"/",mes_velocidade,"/validas/")
  
  nm_frescao <- list.files(path=path_frescao, full.names = T, pattern = "*.csv",recursive = TRUE)
  
  trips_frescao <- map_df(nm_frescao, fread) %>% 
    select(servico,direction_id,datetime_partida,datetime_chegada,distancia_planejada,data) %>% 
    mutate(data = as.Date(data)) %>% 
    mutate(distancia_planejada = distancia_planejada/1000)
  
  trips <- rbindlist(list(trips,trips_frescao))
  
  rm(trips_frescao)
}

load_calendar("../../dados/calendario.json")

criar_sumario_trips <- function(trips, days, service_id) {
  
  trips_filtered <- trips %>% 
    filter(lubridate::wday(data) %in% days) %>% 
    filter(ifelse(lubridate::wday(data) %in% c(2,3,4,5,6),
                   is.bizday(data, "Rio_Janeiro"),
                   TRUE)) %>% 
    select(-c(data)) %>% 
    mutate(
      tempo_viagem_ajustado = case_when(
        servico == '851' & direction_id == 0 ~ as.integer(difftime(as.POSIXct(datetime_chegada),
                                                                   as.POSIXct(datetime_partida),
                                                                   units='secs'))+1800,
        servico == 'SP485' & direction_id == 0 ~ as.integer(difftime(as.POSIXct(datetime_chegada),
                                                                     as.POSIXct(datetime_partida),
                                                                     units='secs'))+1200,
        TRUE ~ as.integer(difftime(as.POSIXct(datetime_chegada),
                                   as.POSIXct(datetime_partida),
                                   units='secs'))+360),
      velocidade_media = round((distancia_planejada*1000)/(tempo_viagem_ajustado)*3.6,2))
  
  trips_filtered_boxplot <- trips_filtered %>%
    group_by(servico,direction_id) %>%
    filter(!tempo_viagem_ajustado %in% boxplot.stats(tempo_viagem_ajustado)$out) %>%
    ungroup() 
  
  trips_summary <- trips_filtered_boxplot %>% 
    group_by(servico,direction_id,hora = lubridate::hour(datetime_partida)) %>% 
    summarise(velocidade = mean(velocidade_media)) %>% 
    rename(trip_short_name = servico) %>% 
    mutate(service_id = service_id)
  
  vel_media <- trips_filtered_boxplot %>% 
    group_by(hora = lubridate::hour(datetime_partida)) %>% 
    summarise(velocidade_geral = mean(velocidade_media)) %>% 
    mutate(service_id = service_id)
  
  list(trips_summary, vel_media)
  
}

trips_summary_list <- criar_sumario_trips(trips, c(2, 3, 4, 5, 6), "U")
sumario_du <- trips_summary_list[[1]]
vel_media_du <- trips_summary_list[[2]]

trips_summary_list <- criar_sumario_trips(trips, 7, "S")
sumario_sab <- trips_summary_list[[1]]
vel_media_sab <- trips_summary_list[[2]]

trips_summary_list <- criar_sumario_trips(trips, 1, "D")
sumario_dom <- trips_summary_list[[1]]
vel_media_dom <- trips_summary_list[[2]]

rm(trips,trips_summary_list)

sumario_combinado <- rbindlist(list(sumario_du,sumario_sab,sumario_dom)) %>% 
  distinct(trip_short_name,direction_id,service_id,hora,.keep_all = T) %>% 
  rename(service_id_join = service_id) %>% 
  mutate(trip_short_name = as.character(trip_short_name))

velocidade_combinado <- rbindlist(list(vel_media_du,vel_media_sab,vel_media_dom)) %>% 
  distinct(service_id,hora,.keep_all = T) %>% 
  rename(service_id_join = service_id)

gtfs <- gtfstools::read_gtfs(endereco_gtfs)

colunas_originais <- colnames(gtfs$stop_times)

stp_tms <- gtfs$stop_times %>% 
  left_join(select(gtfs$trips,trip_id,trip_short_name,direction_id,service_id)) %>% 
  left_join(select(gtfs$frequencies,trip_id,start_time)) %>% 
  group_by(trip_id) %>% 
  mutate(start_time = if_else(is.na(start_time),head(arrival_time,1),start_time),
         horario_inicio = as.ITime(head(arrival_time,1))) %>% 
  ungroup() %>% 
  mutate(hora = if_else(lubridate::hour(lubridate::hms(start_time)) > 23,
                        lubridate::hms(start_time)-lubridate::hours(24),
                        lubridate::hms(start_time))) %>% 
  mutate(hora = paste(sprintf("%02d", lubridate::hour(hora)),
                      sprintf("%02d", lubridate::minute(hora)),
                      sprintf("%02d", lubridate::second(hora)),sep=':')) %>% 
  mutate(hora = as.ITime(hora),
         hora = as.POSIXct(hora),
         hora = lubridate::force_tz(hora, "America/Sao_Paulo"),
         hora = lubridate::round_date(hora,unit="hour"),
         hora = lubridate::hour(hora)) %>% 
  mutate(service_id_join = substr(service_id,1,1),
         service_id_join = if_else(service_id == 'AN31','D',service_id_join)) %>% 
  left_join(sumario_combinado, by=c('trip_short_name','direction_id','service_id_join','hora')) %>% 
  left_join(velocidade_combinado, by=c('service_id_join','hora')) %>% 
  mutate(velocidade_seg = if_else(is.na(velocidade),(velocidade_geral/3.6),(velocidade/3.6))) %>% 
  mutate(chegada = shape_dist_traveled/velocidade_seg) %>% 
  mutate(arrival_time = seconds_to_period(chegada)) %>% 
  mutate(departure_time = paste(sprintf("%02d", lubridate::hour(arrival_time)),
                                sprintf("%02d", lubridate::minute(arrival_time)),
                                sprintf("%02d", as.integer(lubridate::second(arrival_time))),sep=':')) %>% 
  mutate(arrival_time = departure_time) %>% 
  mutate(arrival_time = as.character(as.ITime(arrival_time) + horario_inicio),
         departure_time = arrival_time) %>% 
  mutate(shape_dist_traveled = round(shape_dist_traveled,2)) %>% 
  select(all_of(colunas_originais))

gtfs$stop_times <- stp_tms

endereco_gtfs_proc <- paste0("../../dados/gtfs/",ano_gtfs,"/",gtfs_processar,
                        "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q_PROC.zip")

write_gtfs(gtfs,endereco_gtfs_proc)

