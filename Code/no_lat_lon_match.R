library(sf)
library(dplyr)
library(data.table)
zones_fl_needed<- st_read(
  "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge/storm data/geo data/z_18mr25.shp"
) %>%
  filter(STATE == "FL") %>%              # Florida only
  select(ZONE, geometry) %>%          
  na.omit() %>%
  mutate(ZONE = as.integer(ZONE)) %>%
  st_transform(4326) %>%#transform NAD83 → WGS84
  group_by(ZONE) %>%                     # We may have different gemometry with the same ZONE, so take the union
  summarise(
    geometry = st_union(geometry),
    .groups = "drop"
  )#Public Forecast Zones

storm_no_lat_lon <- fread(
  "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/Storm info/storm_no_lat_lon.csv"
)[
  , .(EVENT_ID, EVENT_TYPE,
      BEGIN_DATE_TIME, END_DATE_TIME,
      CZ_FIPS)
]
#https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/Storm-Data-Export-Format.pdf
# ensure CZ_FIPS is integer
storm_no_lat_lon[, CZ_FIPS := as.integer(CZ_FIPS)]
storm_fl_sf <- zones_fl_needed %>%
  inner_join(
    storm_no_lat_lon,
    by = c("ZONE" = "CZ_FIPS")
  )#Zones  2   4   6  20  22  25  26  32  33  36  37  39  40  42  47  48  49  51  54  55  59  60  62  64  65 107 122 133 222 are missing from Public Forecast Zones shp file


policy_full=readRDS('E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge/ClaimPolicy_FL_join.rds')
policy_needed <- policy_full %>%
  select(
    policyEffectiveDate,
    policyTerminationDate,
    latitude,
    longitude,
    PropertyID2,
    PropertyPolicyID
  )#select required columns
policy_clean <- policy_needed %>%
  filter(
    !is.na(PropertyPolicyID),   
    !is.na(latitude),           
    !is.na(policyEffectiveDate)
  )#remove NA

policy_clean <- policy_clean %>%
  mutate(
    policy_start = as.POSIXct(policyEffectiveDate, tz = "UTC"),
    policy_end   = as.POSIXct(policyTerminationDate, tz = "UTC")
  ) %>% #standarize the format of time
  select(
    PropertyPolicyID,
    PropertyID2,
    policy_start,
    policy_end,
    longitude,
    latitude
  )#remove original time

library(lubridate)
storm_fl_sf <- storm_fl_sf %>%
  mutate(
    event_start = ymd_hm(BEGIN_DATE_TIME, tz = "UTC"),
    event_end   = ymd_hm(END_DATE_TIME, tz = "UTC")
  ) %>%  #standarize the format of time
  select(
    EVENT_ID,
    EVENT_TYPE,
    event_start,
    event_end,
    geometry
  )  #remove original time
poly_wkb <- st_as_binary(st_geometry(storm_fl_sf))#Extracts the sf geometry column and converts each polygon to WKB.
storm_fl_sf <- as.data.frame(st_drop_geometry(storm_fl_sf))#Drops the sf geometry and sf class.
storm_fl_sf$poly_wkb <- poly_wkb#attache the WKB vector as a new column


library(duckdb);library(DBI);library(parallel)
con <- dbConnect(duckdb::duckdb(), 
                 dbdir = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/no_lat_lon_merge.duckdb", 
                 read_only = FALSE)
dbExecute(con, "SET temp_directory = 'E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/tmp'")
dbExecute(con, paste0("SET threads TO ", parallel::detectCores()))
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")


duckdb_register(con, "policies", policy_clean)
duckdb_register(con, "events", storm_fl_sf)

query <- "
  CREATE OR REPLACE TABLE final_matches AS
  SELECT
      p.PropertyPolicyID,
      p.PropertyID2,
      p.policy_start,
      p.policy_end,
      e.EVENT_ID,
      e.EVENT_TYPE,
      e.event_start,
      e.event_end
      FROM policies AS p
     JOIN events   AS e
    ON p.policy_start <= e.event_end
   AND p.policy_end >= e.event_start
   AND ST_Contains(
         ST_GeomFromWKB(e.poly_wkb),
         ST_Point(p.longitude, p.latitude)
       );
"


dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)

con <- dbConnect(duckdb::duckdb(), 
                 dbdir = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/no_lat_lon_merge.duckdb", 
                 read_only = TRUE)
final_matches_df <- dbGetQuery(con, "SELECT * FROM final_matches")
saveRDS(
  final_matches_df, 
  file = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/no_lat_lon_match.rds"
)

no=readRDS('E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/no_lat_lon_match.rds')
yes=readRDS('E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/lat_lon_match.rds')
no$lat_lon <- 0# to show the match comes without lat/lon
yes$lat_lon <- 1#to show the match comes with lat/lon
saveRDS(
  yes, 
  file = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/lat_lon_match_new.rds"
)
saveRDS(
  no, 
  file = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/no_lat_lon_merge/no_lat_lon_match_new.rds"
)
total_new=rbind(yes,no)
total_new <- total_new %>%
  distinct(PropertyPolicyID, EVENT_ID, .keep_all = TRUE)
saveRDS(
  total_new, 
  file = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/total_new.rds"
)