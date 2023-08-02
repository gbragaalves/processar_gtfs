pacman::p_load(gtfstools, dplyr, data.table, Hmisc)

ano_gtfs <- "2023"
mes_gtfs <- "07"
quinzena_gtfs <- "01"

endereco_sppo <- file.path("../../dados/gtfs", ano_gtfs,
  paste0("sppo_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip"))

gtfs_sppo <- read_gtfs(endereco_sppo)

gtfs_sppo$routes <- as.data.table(gtfs_sppo$routes) %>%
  mutate(numero = as.integer(stringr::str_extract(route_short_name, "[0-9]+"))) %>%
  mutate(route_type = if_else(numero > 1000, "200", "700")) %>%
  select(-c(numero))

tarifas_frescao <- fread("../../dados/insumos/frescao/tarifas.csv") %>%
  rename(route_short_name = servico) %>%
  left_join(select(gtfs_sppo$routes, route_short_name, agency_id, route_id)) %>%
  mutate(
    agency_id = case_when(
      agency_id == "22002" ~ "IS",
      agency_id == "22003" ~ "IN",
      agency_id == "22004" ~ "TC",
      agency_id == "22005" ~ "SC"
    ),
    fare_id = paste(fare_id, agency_id, sep = "_")
  ) %>%
  select(-route_short_name, agency_id)

fare_rules <- as.data.table(gtfs_sppo$routes) %>%
  select(route_short_name, route_id, route_type, agency_id) %>%
  filter(route_type == "700") %>%
  mutate(
    fare_id = "BUC",
    agency_id = case_when(
      agency_id == "22002" ~ "IS",
      agency_id == "22003" ~ "IN",
      agency_id == "22004" ~ "TC",
      agency_id == "22005" ~ "SC"
    ),
    fare_id = paste(fare_id, agency_id, sep = "_")
  ) %>%
  select(fare_id, route_id) %>%
  bind_rows(tarifas_frescao)

trips_usar_sppo <- unique(gtfs_sppo$stop_times$trip_id)

trips_fantasma <- fread("../../dados/insumos/trip_id_fantasma.txt") %>%
  unlist()

gtfs_filt_sppo <- filter_by_trip_id(gtfs_sppo, trips_usar_sppo) %>% 
  filter_by_trip_id(trips_fantasma, keep = F)

rm(trips_usar_sppo, trips_fantasma)

fare_rules <- fare_rules %>%
  filter(route_id %in% gtfs_filt_sppo$routes$route_id)

gtfs_filt_sppo$fare_rules <- fare_rules

endereco_brt <- file.path("../../dados/gtfs", ano_gtfs,
  paste0("brt_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip"))

gtfs_brt <- read_gtfs(endereco_brt)

gtfs_brt$routes <- as.data.table(gtfs_brt$routes) %>%
  mutate(route_type = "702")

fare_rules <- as.data.table(gtfs_brt$routes) %>%
  select(route_id) %>%
  mutate(fare_id = "BRT")

gtfs_brt$fare_rules <- fare_rules

rotas <- unique(gtfs_brt$trips$trip_short_name)

routes_usar_brt <- gtfs_brt$routes %>%
  filter(route_short_name %in% rotas) %>%
  pull(route_id)

gtfs_brt$trips <- gtfs_brt$trips %>%
  mutate(service_id = if_else(service_id %in% c("U", "S", "D"), paste0(service_id, "_REG"), service_id))

gtfs_brt$calendar <- gtfs_brt$calendar %>%
  mutate(service_id = if_else(service_id %in% c("U", "S", "D"), paste0(service_id, "_REG"), service_id))

gtfs_brt$calendar_dates <- data.table()
gtfs_brt$feed_info <- data.table()

gtfs_filt_brt <- filter_by_route_id(gtfs_brt, routes_usar_brt)

write_gtfs(gtfs_filt_brt, endereco_brt)

gtfs_combi <- merge_gtfs(gtfs_filt_sppo, gtfs_filt_brt)

pontos_apagar <- gtfs_combi$stops %>%
  filter(stop_name == "APAGAR") %>%
  select(stop_id, stop_name, stop_lat, stop_lon)

gtfs_combi$agency <- as.data.table(gtfs_combi$agency) %>%
  select(-c(agency_phone, agency_fare_url, agency_email, agency_branding_url))

gtfs_combi$feed_info <- as.data.table(gtfs_combi$feed_info) %>%
  select(-c(feed_version, default_lang, feed_contact_url, feed_id))

gtfs_combi$routes <- as.data.table(gtfs_combi$routes) %>%
  select(-c(route_sort_order, continuous_pickup, route_branding_url, continuous_drop_off, route_url)) %>%
  mutate(route_color = case_when(
    agency_id == "22002" ~ "FCC417",
    agency_id == "22003" ~ "A2B71A",
    agency_id == "22004" ~ "0C6FA8",
    agency_id == "22005" ~ "E31919",
    TRUE ~ route_color
  )) %>%
  mutate(route_color = if_else(route_type == 200, "030478", route_color)) %>%
  mutate(route_text_color = case_when(
    agency_id == "22002" ~ "000000",
    agency_id == "22003" ~ "000000",
    agency_id == "22004" ~ "ffffff",
    agency_id == "22005" ~ "ffffff",
    route_type == "200" ~ "000000",
    TRUE ~ route_text_color
  )) %>%
  mutate(route_text_color = if_else(route_type == 200, "ffffff", route_text_color))

gtfs_combi$trips <- as.data.table(gtfs_combi$trips) %>%
  select(-c(block_id, wheelchair_accessible, bikes_allowed))

gtfs_combi$calendar <- gtfs_combi$calendar %>%
  distinct_all()

gtfs_combi$calendar_dates <- gtfs_combi$calendar_dates %>%
  distinct_all()

gtfs_combi$stops <- gtfs_combi$stops %>%
  distinct(stop_id, .keep_all = T) %>%
  filter(!(stop_id %in% pontos_apagar$stop_id))

gtfs_combi$feed_info <- gtfs_combi$feed_info[1, ]

gtfs_combi$shapes <- as.data.table(gtfs_combi$shapes) %>%
  mutate(shape_dist_traveled = round(shape_dist_traveled, 2)) %>%
  group_by(shape_id) %>%
  distinct(shape_dist_traveled, .keep_all = T) %>%
  arrange(shape_dist_traveled) %>%
  mutate(shape_pt_sequence = 1:n()) %>%
  ungroup() %>%
  arrange(shape_id)

gtfs_combi$stop_times <- as.data.table(gtfs_combi$stop_times) %>%
  mutate(timepoint = 0) %>%
  select(-c(pickup_type, drop_off_type, continuous_pickup, continuous_drop_off)) %>%
  mutate(shape_dist_traveled = round(shape_dist_traveled, 2)) %>%
  filter(!(stop_id %in% pontos_apagar$stop_id))

gtfs_combi$fare_attributes <- as.data.table(gtfs_combi$fare_attributes) %>%
  mutate(currency_type = "BRL")

gtfs_combi$trips <- as.data.table(gtfs_combi$trips) %>%
  mutate(trip_headsign = if_else(trip_headsign == "", "Circular", trip_headsign))

endereco_gtfs_combi <- paste0(
  "../../dados/gtfs/", ano_gtfs, "/gtfs_combi",
  "_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.zip"
)

validacao_qrcode <- gtfs_combi$stops %>%
  filter(is.na(location_type))

View(validacao_qrcode)

write_gtfs(gtfs_combi, endereco_gtfs_combi)

write_gtfs(gtfs_combi, paste0("../../dados/gtfs/", ano_gtfs, "/gtfs_rio-de-janeiro.zip"))
