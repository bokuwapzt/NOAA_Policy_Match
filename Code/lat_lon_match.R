library(dplyr)
storm_with_lat_lon=read.csv('E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/Storm info/storm_lat_lon.csv')
storm_with_lat_lon<- storm_with_lat_lon %>%
  select(EVENT_ID, EVENT_TYPE,
         BEGIN_DATE_TIME, END_DATE_TIME,
         BEGIN_LAT, BEGIN_LON, END_LAT, END_LON)

library(sf)
library(dplyr)



radius_by_type <- function(event_type) {
  et <- tolower(trimws(event_type))
  if (et == "debris flow") return(200)       
  if (et == "flash flood") return(1000)
  if (et == "flood")        return(2000)
  if (et == "heavy rain")   return(10000)
}#buffer radius

df <- storm_with_lat_lon %>%
  mutate(
    BEGIN_LON = as.numeric(BEGIN_LON),
    BEGIN_LAT = as.numeric(BEGIN_LAT),
    END_LON   = as.numeric(END_LON),
    END_LAT   = as.numeric(END_LAT)
  )

make_geom <- function(r) {
  p1 <- c(r$BEGIN_LON, r$BEGIN_LAT) 
  p2 <- c(r$END_LON,r$END_LAT)
  
  if (any(is.na(c(p1, p2)))) {
    # Return an empty sfg geometry
    return(st_geometrycollection()) 
  }
  if (isTRUE(all.equal(p1, p2))) {
    # Return the sfg geometry
    st_point(p1) 
  } else {
    # Return the sfg geometry
    st_linestring(rbind(p1, p2)) 
  }
}#Build one geometry per row. If start==end → POINT. Else → LINESTRING from start to end. Returns sfg objects

geoms <- lapply(seq_len(nrow(df)), function(i) make_geom(df[i, ]))#Wraps the list of sfg into an sfc (a column of geometries) with CRS EPSG:4326 (WGS84 lon/lat).
g <- st_sf(df, geom = st_sfc(geoms, crs = 4326)) #Combines attributes + geometry into an sf data frame g

buffer_row <- function(i) {
  row <- g[i, ]
  r_m <- radius_by_type(row$EVENT_TYPE)
  geom_conus <- st_transform(row$geom, 5070)     # 4326(lat/lon) -> 5070 (meters)
  buf_conus  <- st_buffer(geom_conus, dist = r_m) # buffer in meters. If geom_conus is a POINT: It creates a circle with a radius of r_m meters. If geom_conus is a LINE: It creates a capsule or "sausage" shape by buffering r_m meters on all sides of the line.
  buf_4326   <- st_transform(buf_conus, 4326)     # back to 4326(lat/lon)

  
  out <- st_sf(st_drop_geometry(row), affected_poly = st_geometry(buf_4326))#delete point/line object and add affected polygon object
}

affected_list <- lapply(seq_len(nrow(g)), buffer_row)
affected <- do.call(rbind, affected_list)


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
affected <- affected %>%
  mutate(
    event_start = ymd_hm(BEGIN_DATE_TIME, tz = "UTC"),
    event_end   = ymd_hm(END_DATE_TIME, tz = "UTC")
  ) %>%  #standarize the format of time
  select(
    EVENT_ID,
    EVENT_TYPE,
    event_start,
    event_end,
    affected_poly
  )  #remove original time
affected$poly_wkb <- st_as_binary(affected$affected_poly)#converts the spatial polygons into a binary format that databases can read
affected <- st_drop_geometry(affected)#remove the original, active geometry column (affected_poly)

library(duckdb);library(DBI);library(parallel)
con <- dbConnect(duckdb::duckdb(), 
                 dbdir = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/lat_lon_merge.duckdb", 
                 read_only = FALSE)
dbExecute(con, "SET temp_directory = 'E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/tmp'")
dbExecute(con, paste0("SET threads TO ", parallel::detectCores()))
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")


duckdb_register(con, "policies", policy_clean)
duckdb_register(con, "events", affected)

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
                 dbdir = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/lat_lon_merge.duckdb", 
                 read_only = TRUE)
final_matches_df <- dbGetQuery(con, "SELECT * FROM final_matches")
saveRDS(
  final_matches_df, 
  file = "E:/迅雷云盘/笔记本/laptop-E/graduate/25 fall/RA/data merge2/lat_lon_merge/lat_lon_match.rds"
)


