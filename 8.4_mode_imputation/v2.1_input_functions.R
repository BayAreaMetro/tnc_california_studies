# input_functions.R

# 8 Oct 2020
# matt.landis@rsginc.com

# Contains all of the model inputs that define the model specification
# Intended to be sourced.

# Long data set (precursor data.table) =========================================

create_base_dataset = function(hh, person, trip, location){
  
  ex_hh = copy(hh)
  ex_person = copy(person)
  ex_trip = copy(trip)
  ex_location = copy(location)
  
  # Fix missing values in travel_date_dow
  # all of them are in early morning hours, need to have day shifted back one
  
  ex_trip[is.na(travel_date_dow), 
          travel_date_dow := lubridate::wday(depart_time, week_start=1) -
            1 * depart_hour %between% c(0, 2)]
  ex_trip[travel_date_dow == 0, travel_date_dow2 := 7]
  
  # -	Trip distance band (e.g. breakpoints of 0.1m, 0.25m, 1m, 5m, 10m, 25m, 50m)
  # -	Trip speed band (e.g. breakpoints of 2, 5, 10, 20, 25, 50, 80 mph)
  # -	The mode of the previous trip
  # -	The mode of the next trip
  # -	Whether or not the trip origin or destination is home (people are less likely to switch modes between trips if there is no stop at home in between - especially for car)
  # -	The percent of trips by each mode type made by that person the same day
  # -	The percent of trips by each mode type made by that person the same week
  
  trip_variables = ex_trip[, 
                           .(hh_id, person_id, trip_id, 
                             person_num, day_num, trip_num, 
                             mode_type, 
                             distance_miles = distance, 
                             dwell_time_min, 
                             travel_date_dow, 
                             depart_hour, 
                             arrive_hour,
                             d_distance_home,
                             d_distance_work,
                             d_distance_school, 
                             o_county_fips,
                             d_county_fips,
                             added_trip,
                             d_purpose_imputed,
                             d_purpose_category_imputed,
                             o_purpose_imputed,
                             o_purpose_category_imputed,
                             mode_1, mode_2, mode_3,
                             unlinked_trip, unlinked_split,
                             survey_complete_trip,
                             split_due_to_loop, 
                             speed_mph,
                             distance)]
  
  
  # calculation of implied speed between points 2, 3, ..., N
  # Merge trip and location tables
  ex_location = (ex_trip[added_trip == 0, .(trip_id, depart_time, depart_time_imputed, arrive_time)])[
    ex_location, on = .(trip_id)]
  
  ex_location[,      is_origin := 1 * (depart_time == collected_at)]
  ex_location[, is_destination := 1 * (arrive_time == collected_at)]
  
  ex_location[, is_origin_imputed := 1 * (depart_time_imputed == collected_at_imputed)]
  ex_location[, is_destination_imputed := 1 * (arrive_time == collected_at_imputed)]
  
  # Sort by collected_at (and by implication, collected_at_imputed)
  stopifnot(ex_location[order(trip_id, collected_at) != order(trip_id, collected_at_imputed), .N] == 0)
  ex_location = ex_location[order(trip_id, collected_at)]
  
  ex_location[, 
              dist_to_prev := 
                get_distance_meters(
                  c(lon, lat), 
                  c(shift(lon, type = 'lag'), shift(lat, type = 'lag'))), 
              by = .(trip_id)]
  
  # Filter location table for duplicate points
  ex_location = ex_location[dist_to_prev > 0 | is_origin == 1 | is_destination == 1]
  
  message('Calculating dist_to_next')
  ex_location[, 
              dist_to_next := 
                get_distance_meters(
                  c(lon, lat), 
                  c(shift(lon, type = 'lead'), shift(lat, type = 'lead'))), 
              by = .(trip_id)]
  
  message('Calculating time_to_next')
  ex_location[, 
              time_to_next := 
                as.numeric(difftime(shift(collected_at, type = 'lead'),
                                    collected_at, units = 'secs')),
              by = .(trip_id)]
  
  message('Calculating time_to_next_imputed')
  ex_location[, 
              time_to_next_imputed := 
                as.numeric(difftime(shift(collected_at_imputed, type = 'lead'),
                                    collected_at_imputed, units = 'secs')),
              by = .(trip_id)]
  
  ex_location[, speed_ms := dist_to_next / time_to_next]
  ex_location[, speed_ms_imputed := dist_to_next / time_to_next_imputed]
  
  # calculate % of trip at each speed
  ex_location[, `:=` (
    speed_0_2 = 0,
    speed_2_4 = 0,
    speed_4_15 = 0,
    speed_15_40 = 0,
    speed_40_plus = 0
  )]
  
  # convert to mph from meters per sec
  ex_location[, speed_mph_imp := speed_ms_imputed * 2.23694]
  
  ex_location = 
    ex_location[
      is_origin != 1 & is_destination != 1 & speed_ms_imputed > 0, 
      .(median_speed_ms_imp = median(speed_ms_imputed)), 
      .(trip_id)][ex_location, on = .(trip_id)]
  
  ex_location = 
    ex_location[
      is_origin != 1 & is_destination != 1 & speed_ms > 0, 
      .(median_speed_ms = median(speed_ms)), 
      .(trip_id)][ex_location, on = .(trip_id)]
  
  ex_location[speed_mph_imp < 2, speed_0_2 := 1]
  ex_location[speed_mph_imp >= 2 & speed_mph_imp < 4, speed_2_4 := 1]
  ex_location[speed_mph_imp >= 4 & speed_mph_imp < 15, speed_4_15 := 1]
  ex_location[speed_mph_imp >= 15 & speed_mph_imp < 40, speed_15_40 := 1]
  ex_location[speed_mph_imp >= 40, speed_40_plus := 1]
  
  ex_location[, perc_dist := 
                dist_to_next / sum(dist_to_next, na.rm = TRUE),
              by = .(trip_id)]
  
  ex_location[, perc_time :=
                time_to_next / sum(time_to_next, na.rm=TRUE),
              by = .(trip_id)]
  
  ex_location[, `:=` (
    p_speed_dist_0_2 = sum(speed_0_2 * perc_dist, na.rm = TRUE),
    p_speed_dist_2_4 = sum(speed_2_4 * perc_dist, na.rm = TRUE),
    p_speed_dist_4_15 = sum(speed_4_15 * perc_dist, na.rm = TRUE),
    p_speed_dist_15_40 = sum(speed_15_40 * perc_dist, na.rm = TRUE),
    p_speed_dist_40_plus = sum(speed_40_plus * perc_dist, na.rm = TRUE)
  ), .(trip_id)]
  
  ex_location[, `:=` (
    p_speed_time_0_2 = sum(speed_0_2 * perc_time, na.rm = TRUE),
    p_speed_time_2_4 = sum(speed_2_4 * perc_time, na.rm = TRUE),
    p_speed_time_4_15 = sum(speed_4_15 * perc_time, na.rm = TRUE),
    p_speed_time_15_40 = sum(speed_15_40 * perc_time, na.rm = TRUE),
    p_speed_time_40_plus = sum(speed_40_plus * perc_time, na.rm = TRUE)
  ), .(trip_id)]
  
  location_variables = 
    ex_location[, 
                .(num_points = .N),
                .(trip_id, 
                  median_speed_mph = median_speed_ms * 2.23694,
                  median_speed_mph_imp = median_speed_ms_imp * 2.23694,
                  p_speed_dist_0_2,
                  p_speed_dist_2_4,
                  p_speed_dist_4_15,
                  p_speed_dist_15_40,
                  p_speed_dist_40_plus,
                  p_speed_time_0_2,
                  p_speed_time_2_4,
                  p_speed_time_4_15,
                  p_speed_time_15_40,
                  p_speed_time_40_plus)] 
  
  hh_variables = 
    ex_hh[, 
          .(hh_id, 
            num_vehicles, 
            num_people, 
            num_full_time_workers, 
            num_part_time_workers, 
            income_aggregate, 
            home_bg_geoid, 
            home_county_fips)]
  
  person_variables = 
    ex_person[, 
              .(person_id, 
                worker, 
                license, 
                student, 
                age, 
                disability, 
                tnc_freq, 
                transit_freq)]
  
  # create data.table we will use in model
  dt_base = location_variables[trip_variables, on = .(trip_id)]
  
  dt_base = hh_variables[dt_base, on = .(hh_id)]
  
  dt_base[, person_id := as.character(person_id)]
  person_variables[, person_id := as.character(person_id)]
  dt_base = person_variables[dt_base, on = .(person_id)]
  
  dt_base = dt_base[order(person_id, day_num, trip_num)]
  
  # distribution of choices
  dt_base[, .N, mode_type][order(mode_type)]
  
  dt_base[mode_type == 1,  mode_type_condensed := 1]
  dt_base[mode_type == 2 &  (mode_1 %in% 2:4 | mode_2 %in% 2:4 | mode_3 %in% 2:4), mode_type_condensed := 2]
  dt_base[mode_type == 2 & !(mode_1 %in% 2:4 | mode_2 %in% 2:4 | mode_3 %in% 2:4), mode_type_condensed := 6]
  dt_base[mode_type == 3,  mode_type_condensed := 3]
  dt_base[mode_type == 4,  mode_type_condensed := 6]
  dt_base[mode_type == 5,  mode_type_condensed := 4]
  dt_base[mode_type == 6,  mode_type_condensed := 6]
  dt_base[mode_type == 7,  mode_type_condensed := 6]
  dt_base[mode_type == 8,  mode_type_condensed := 6]
  dt_base[mode_type == 9,  mode_type_condensed := 5]
  dt_base[mode_type == 10, mode_type_condensed := 6]
  dt_base[mode_type == 11, mode_type_condensed := 6]
  dt_base[mode_type == 12, mode_type_condensed := 6]
  dt_base[mode_type == 13, mode_type_condensed := 6]
  dt_base[mode_type == -9998, mode_type_condensed := -9998]
  
  dt_base[, .N, .(mode_type, mode_type_condensed)][order(mode_type, mode_type_condensed)][order(mode_type_condensed, mode_type)]
  
  dt_base[, .(distance_miles)][order(distance_miles)]
  dt_base[, .(median_speed_mph)]
  
  dt_base[, prev_mode_type := shift(mode_type_condensed, type = "lag", n = 1), .(person_id, day_num)]
  dt_base[, next_mode_type := shift(mode_type_condensed, type = "lead", n = 1), .(person_id, day_num)]
  
  dt_base[, .N, .(prev_match = prev_mode_type == mode_type_condensed)]
  dt_base[, .N, .(next_match = next_mode_type == mode_type_condensed)]
  
  dt_base[, .N, .(mode_type_condensed, prev_match = prev_mode_type == mode_type_condensed)][order(mode_type_condensed, prev_match)]
  
  dt_base[, num_trips_1 := sum(mode_type_condensed == 1), .(person_id)]
  dt_base[, num_trips_2 := sum(mode_type_condensed == 2), .(person_id)]
  dt_base[, num_trips_3 := sum(mode_type_condensed == 3), .(person_id)]
  dt_base[, num_trips_4 := sum(mode_type_condensed == 4), .(person_id)]
  dt_base[, num_trips_5 := sum(mode_type_condensed == 5), .(person_id)]
  dt_base[, num_trips_6 := sum(mode_type_condensed == 6), .(person_id)]
  dt_base[, num_trips_9998 := sum(mode_type_condensed == -9998), .(person_id)]
  
  dt_base[is.na(num_trips_1), num_trips_1 := 0]
  dt_base[is.na(num_trips_2), num_trips_2 := 0]
  dt_base[is.na(num_trips_3), num_trips_3 := 0]
  dt_base[is.na(num_trips_4), num_trips_4 := 0]
  dt_base[is.na(num_trips_5), num_trips_5 := 0]
  dt_base[is.na(num_trips_6), num_trips_6 := 0]
  dt_base[is.na(num_trips_9998), num_trips_9998 := 0]
  
  dt_base[, 
                  num_trips := 
                    num_trips_1 + 
                    num_trips_2 + 
                    num_trips_3 + 
                    num_trips_4 +
                    num_trips_5 +
                    num_trips_6 +
                    num_trips_9998, .(person_id)]
  
  dt_base[, perc_trips_1 := num_trips_1 / num_trips]
  dt_base[, perc_trips_2 := num_trips_2 / num_trips]
  dt_base[, perc_trips_3 := num_trips_3 / num_trips]
  dt_base[, perc_trips_4 := num_trips_4 / num_trips]
  dt_base[, perc_trips_5 := num_trips_5 / num_trips]
  dt_base[, perc_trips_6 := num_trips_6 / num_trips]
  
  dt_base[, .N, perc_trips_1][order(perc_trips_1)]
  dt_base[, .N, perc_trips_2][order(perc_trips_2)]
  
  days_to_remove = dt_base[survey_complete_trip == 0, .N, .(person_id, day_num)][, .(person_id, day_num, day_to_remove = 1)]
  
  dt_base = days_to_remove[dt_base, on = .(person_id, day_num), allow.cartesian = TRUE]
  
  dt_base[is.na(day_to_remove), day_to_remove := 0]
  
  # mode_type_condensed
  # 1: Walk
  # 2: Bike
  # 3: Car
  # 4: Transit
  # 5: TNC
  # 6: Other
  
  return(dt_base)
}


# Create input variables ======================================================

create_input_dataset = function(dt_base, county_fips_in_region, predict=FALSE){
  
  dt = copy(dt_base)
  
  missing_modes = c(-9998, 997, 995)
  
  if (!predict) {
    nrows = nrow(dt)
    
    dt = dt[
      mode_type_condensed %in% 1:5 &
        day_to_remove != 1 &
        d_county_fips %in% county_fips_in_region &
        o_county_fips %in% county_fips_in_region
    ]
    
    # Drop multi-modal trips
    dt[, .N, mode_2 %in% missing_modes]
    dt[, .N, mode_3 %in% missing_modes]
    dt = dt[mode_2 %in% missing_modes & mode_3 %in% missing_modes]
    
  }
  
  
  input = new.env()
  
  with(input, {

    # choice variables
    choice_1 = dt[, mode_type_condensed] == 1
    choice_2 = dt[, mode_type_condensed] == 2
    choice_3 = dt[, mode_type_condensed] == 3
    choice_4 = dt[, mode_type_condensed] == 4
    choice_5 = dt[, mode_type_condensed] == 5
    
    # independent variables in the model
    added_trip = dt[, added_trip]
    has_3_points_or_fewer = dt[, num_points < 4]
    
    has_license     = dt[, license] == 1
    missing_license = dt[, license] == -9998
    
    has_disability         = dt[, disability] == 1
    has_disability_pnta    = dt[, disability] == 999
    has_disability_missing = dt[, disability] == -9998
    
    # Drop imp because all speeds from now on are imputed unless otherwise noted
    median_speed_mph = dt[, median_speed_mph_imp]
    
    # Use non-imputed speed where we have few points
    median_speed_mph[has_3_points_or_fewer] = dt[, speed_mph][has_3_points_or_fewer]
    median_speed_mph[is.na(median_speed_mph)] = 0
    median_speed_mph[median_speed_mph > 80] = 80 # capping extreme speeds
    median_speed_mph_log = log(1 + median_speed_mph)
    
    speed_15plus = median_speed_mph >= 15
    speed_05plus = median_speed_mph >= 5
    speed_lt3 = median_speed_mph <= 3
    
    speed_lt3_non_imp = dt[, median_speed_mph] <= 3 # This uses non-imputed
    speed_lt3_non_imp[is.na(speed_lt3_non_imp)] = TRUE
    
    # speed distribution
    # Set to zero if has 3 points or fewer
    p_speed_dist_0_2 = dt[, p_speed_dist_0_2] * (!has_3_points_or_fewer)
    p_speed_dist_2_4 = dt[, p_speed_dist_2_4] * (!has_3_points_or_fewer)
    p_speed_dist_4_15 = dt[, p_speed_dist_4_15] * (!has_3_points_or_fewer)
    p_speed_dist_15_40 = dt[, p_speed_dist_15_40 ] * (!has_3_points_or_fewer)
    p_speed_dist_40_plus = dt[, p_speed_dist_40_plus ] * (!has_3_points_or_fewer)
    
    p_speed_time_0_2 = dt[, p_speed_time_0_2] * (!has_3_points_or_fewer)
    # p_speed_time_2_4 = dt[, p_speed_time_2_4]
    # p_speed_time_4_15 = dt[, p_speed_time_4_15]
    # p_speed_time_15_40 = dt[, p_speed_time_15_40 ]
    # p_speed_time_40_plus = dt[, p_speed_time_40_plus ]
    
    # Check
    dt[has_3_points_or_fewer == 1, range(p_speed_dist_0_2)]
    dt[has_3_points_or_fewer == 0, range(p_speed_dist_0_2)]
    
    stopifnot(max(p_speed_dist_0_2[has_3_points_or_fewer == 1]) == 0)
    
    
    dt[, .N, num_vehicles]
    
    has_vehicle_1 = dt[ , num_vehicles] == 1
    has_vehicle_2 = dt[ , num_vehicles] == 2
    has_vehicle_3 = dt[ , num_vehicles] >= 3 & dt[ , num_vehicles] != 995
    
    distance = dt[, distance_miles]
    
    distance_greater_than_3 = distance > 3
    
    distance[distance > 100] = 100 # capping extreme distances
    distance_log = log(1 + distance)
    
    distance_for_walk = pmin(distance, 3)
    distance_for_walk_log = log(1 + distance_for_walk)
    
    weekday_trip   = dt[, travel_date_dow] <= 4 # M - Th
    weekend_trip   = dt[, travel_date_dow] > 4  # Fri - Sunday
    friday_trip    = dt[, travel_date_dow] == 5
    saturday_trip  = dt[, travel_date_dow] == 6
    
    d_distance_home = dt[, d_distance_home]
    
    dt[, .N, arrive_hour][order(arrive_hour)]
    
    dt[, .N, num_vehicles / num_people]
    
    age = dt[, age]
    
    dt[, .N, age][order(age)]
    
    age_67    = age %in% 6:7
    age_8plus = age %in% 8:10
    
    age = c(rep(NA,3),21,30,40,50,60,70,80)[age]
    
    age_log = log(age)
    
    income = dt[, income_aggregate]
    
    income[income == -9998] = NA
    income[income == 999] = NA
    
    income = c(12.5,32.5,62.5,87.5,175,300)[income]
    
    income[is.na(income)] = mean(income, na.rm = TRUE)
    
    income_log = log(income)
    
    arrive_before_6 = dt[, arrive_hour] <= 6
    arrive_after_20 = dt[, arrive_hour] >= 20
    
    arrive_7thru9 = dt[, arrive_hour] %in% 7:9
    arrive_16thru19 = dt[, arrive_hour] %in% 16:19
    
    # 6-7 days a week
    # 5 days a week
    # 4 days a week
    # 2-3 days a week
    # 1 day a week
    # 1-3 days per month
    # Less than monthly
    # Never
    
    dt[, .N, .(transit_freq)][order(transit_freq)]
    
    transit_freq = dt[, transit_freq]
    
    transit_weekly_4plus = transit_freq <= 3
    transit_weekly_1to3  = transit_freq > 3 & transit_freq <= 5
    transit_monthly      = transit_freq > 5 & transit_freq <= 7
    
    dt[, .N, .(tnc_freq)][order(tnc_freq)]
    
    tnc_freq = dt[, tnc_freq]
    
    tnc_weekly_4plus = tnc_freq <= 3
    tnc_weekly_1to3  = tnc_freq > 3 & tnc_freq <= 5
    tnc_monthly      = tnc_freq > 5 & tnc_freq <= 7
    
    d_county_fips = dt[, d_county_fips]
    
    
    # County specific variables
    
    d_county_fips_001 = d_county_fips == '001' # TNC_Bayarea
    d_county_fips_075 = d_county_fips == '075' # TNC_Bayarea
    d_county_fips_085 = d_county_fips == '085' # TNC_Bayarea
    d_county_fips_037 = d_county_fips == '037' # TNC_SCAG
      
    worker = dt[, worker] == 1
    
    # (1) walk (mode_type=1),  
    # (2) bike (mode_type=2 and mode_1, mode_2 or mode_3 = 2, 3 or 4) - does not include bike-share, scooter-share, moped-share
    # (3) car (mode_type=3)
    # (4) transit (mode_type=5)
    # (5) TNC (mode_type=9)
    # (6) other (all other cases)
    
    
    prev_inertia_1 = dt[, prev_mode_type] == 1
    prev_inertia_2 = dt[, prev_mode_type] == 2
    prev_inertia_3 = dt[, prev_mode_type] == 3
    prev_inertia_4 = dt[, prev_mode_type] == 4
    prev_inertia_5 = dt[, prev_mode_type] == 5
    
    next_inertia_1 = dt[, next_mode_type] == 1
    next_inertia_2 = dt[, next_mode_type] == 2
    next_inertia_3 = dt[, next_mode_type] == 3
    next_inertia_4 = dt[, next_mode_type] == 4
    next_inertia_5 = dt[, next_mode_type] == 5
    
    prev_inertia_1[is.na(prev_inertia_1)] = FALSE
    prev_inertia_2[is.na(prev_inertia_2)] = FALSE
    prev_inertia_3[is.na(prev_inertia_3)] = FALSE
    prev_inertia_4[is.na(prev_inertia_4)] = FALSE
    prev_inertia_5[is.na(prev_inertia_5)] = FALSE
    
    next_inertia_1[is.na(next_inertia_1)] = FALSE
    next_inertia_2[is.na(next_inertia_2)] = FALSE
    next_inertia_3[is.na(next_inertia_3)] = FALSE
    next_inertia_4[is.na(next_inertia_4)] = FALSE
    next_inertia_5[is.na(next_inertia_5)] = FALSE
    
    close_to_home_1 = dt[, d_distance_home <= 1600]
    close_to_home_2 = dt[, d_distance_home > 1600 & d_distance_home <= (5 * 1600)]
    close_to_work_1 = dt[, d_distance_work <= 1600]
    close_to_work_2 = dt[, d_distance_work > 1600 & d_distance_work <= (5 * 1600)]
    
    close_to_home_1[is.na(close_to_home_1)] = FALSE
    close_to_home_2[is.na(close_to_home_2)] = FALSE
    
    close_to_work_1[is.na(close_to_work_1)] = FALSE
    close_to_work_2[is.na(close_to_work_2)] = FALSE
    
    
    perc_walk_trips_1 = dt[, perc_trips_1  < 0.25]
    perc_walk_trips_2 = dt[, perc_trips_1 >= 0.25 & perc_trips_1 < 0.5]
    perc_walk_trips_3 = dt[, perc_trips_1 >= 0.50 & perc_trips_1 < 0.75]
    perc_walk_trips_4 = dt[, perc_trips_1 >= 0.75]
    
    perc_bike_trips_1 = dt[, perc_trips_2  < 0.25]
    perc_bike_trips_2 = dt[, perc_trips_2 >= 0.25 & perc_trips_2 < 0.5]
    perc_bike_trips_3 = dt[, perc_trips_2 >= 0.50 & perc_trips_2 < 0.75]
    perc_bike_trips_4 = dt[, perc_trips_2 >= 0.75]
    
    perc_car_trips_1 = dt[, perc_trips_3  < 0.25]
    perc_car_trips_2 = dt[, perc_trips_3 >= 0.25 & perc_trips_3 < 0.5]
    perc_car_trips_3 = dt[, perc_trips_3 >= 0.50 & perc_trips_3 < 0.75]
    perc_car_trips_4 = dt[, perc_trips_3 >= 0.75]
    
    perc_transit_trips_1 = dt[, perc_trips_4  < 0.25]
    perc_transit_trips_2 = dt[, perc_trips_4 >= 0.25 & perc_trips_4 < 0.5]
    perc_transit_trips_3 = dt[, perc_trips_4 >= 0.50 & perc_trips_4 < 0.75]
    perc_transit_trips_4 = dt[, perc_trips_4 >= 0.75]
    
    perc_tnc_trips_1 = dt[, perc_trips_5  < 0.25]
    perc_tnc_trips_2 = dt[, perc_trips_5 >= 0.25 & perc_trips_5 < 0.5]
    perc_tnc_trips_3 = dt[, perc_trips_5 >= 0.50 & perc_trips_5 < 0.75]
    perc_tnc_trips_4 = dt[, perc_trips_5 >= 0.75]
    
    # purpose related
    d_got_gas    = dt[, d_purpose_imputed == 31]
    o_got_gas    = dt[, o_purpose_imputed == 31]
    d_exercising = dt[, d_purpose_imputed == 51]
    d_shopping   = dt[, d_purpose_category_imputed == 6]
    d_change_mode = dt[, d_purpose_category_imputed == 10]
    o_change_mode = dt[, o_purpose_category_imputed == 10]
    d_escort = dt[, d_purpose_category_imputed == 5]
    o_escort = dt[, o_purpose_category_imputed == 5]
    d_going_home = dt[, d_purpose_category_imputed == 1]
    o_from_home = dt[, o_purpose_category_imputed == 1]
    d_work_related = dt[, d_purpose_category_imputed == 3]
    d_social = dt[, d_purpose_category_imputed == 8]
    
    o_from_home[is.na(o_from_home)] = FALSE
    o_escort[is.na(o_escort)] = FALSE
    o_change_mode[is.na(o_change_mode)] = FALSE
    o_got_gas[is.na(o_got_gas)] = FALSE
    
    split_due_to_loop = dt[, split_due_to_loop]
    
    unlinked = dt[, unlinked_trip] 
    unlinked_split = dt[, unlinked_split] 
    
    # For prediction
    trip_id = dt[, trip_id]
    mode_type_condensed = dt[, mode_type_condensed]
    
    # For apollo
    ID = dt[, person_id]
    
  })
  
  
  dt_input = input %>%
    as.list() %>%
    as.data.table()
  
  # availability
  dt_input[, av_walk := 1]
  dt_input[, av_bike := 1]
  dt_input[, av_car := 1]
  dt_input[, av_transit := 1]
  dt_input[, av_tnc := 1]
  
  # Rearrange column order more conveniently
  
  setcolorder(dt_input, sort(names(dt_input)))
  setcolorder(dt_input, intersect(names(dt), names(dt_input)))
  setcolorder(dt_input, 'trip_id')
  return(dt_input)
  
}


### Vector with names (in quotes) of parameters to be kept fixed at their starting value in apollo_beta, use apollo_beta_fixed = c() if none

apollo_fixed = 
  c("b_unlinked_split_1",
    "b_unlinked_split_4",
    "b_distance_greater_than_3",
    "b_distance_log_1",
    "b_p_speed_dist_4_15_1",
    "b_p_speed_dist_4_15_2",
    "b_p_speed_dist_4_15_4",
    "b_p_speed_dist_4_15_5")

# Fix county coefs
# Which county coefs should be fixed at zero?
county_coefs = paste0('b_d_county_fips_',
                      c(bayarea1='001',
                        bayarea2='075',
                        bayarea3='085',
                        scag1='037'))
alts = c(1, 2, 4, 5)

county_coefs = switch(dbname,
       tnc_bayarea = county_coefs[4],
       tnc_sandag = county_coefs,
       tnc_scag = county_coefs[1:3]
)

alts = rep(alts, each=length(county_coefs))
apollo_fixed = c(apollo_fixed, paste0(county_coefs, '_', alts))


apollo_probabilities =
  function(
    apollo_beta, 
    apollo_inputs, 
    functionality = "estimate"){
    
    ### Attach inputs and detach after function exit
    apollo_attach(apollo_beta, apollo_inputs)
    on.exit(apollo_detach(apollo_beta, apollo_inputs))
    
    b_speed_1 = b_speed_1 + b_speed_3points_1 * has_3_points_or_fewer
    b_speed_2 = b_speed_2 + b_speed_3points_2 * has_3_points_or_fewer
    b_speed_4 = b_speed_4 + b_speed_3points_4 * has_3_points_or_fewer
    b_speed_5 = b_speed_5 + b_speed_3points_5 * has_3_points_or_fewer
    b_speed_log_1 = b_speed_log_1 + b_speed_log_3points_1 * has_3_points_or_fewer
    b_speed_log_2 = b_speed_log_2 + b_speed_log_3points_2 * has_3_points_or_fewer
    b_speed_log_4 = b_speed_log_4 + b_speed_log_3points_4 * has_3_points_or_fewer
    b_speed_log_5 = b_speed_log_5 + b_speed_log_3points_5 * has_3_points_or_fewer
    b_speed_05_1 = b_speed_05_1 + b_speed_05_3points_1 * has_3_points_or_fewer
    b_speed_15_2 = b_speed_15_2 + b_speed_15_3points_2 * has_3_points_or_fewer
    b_speed_lt3_non_imp_3 = b_speed_lt3_non_imp_3 + b_speed_lt3_ni_3points_3 * has_3_points_or_fewer
    
    ### Create list of probabilities P
    P = list()
    
    ### List of utilities: these must use the same names as in mnl_settings, order is irrelevant
    V = list()
    # browser()
    
    V[['walk']] = 
      asc_1 + 
      b_added_trip_1 * added_trip +
      b_age67_1 * age_67 + b_age8plus_1 * age_8plus + 
      b_income_1 * income + b_log_income_1 * income_log + 
      b_speed_1 * median_speed_mph + 
      b_speed_log_1 * median_speed_mph_log + 
      b_speed_05_1 * speed_05plus +
      
      # b_p_speed_dist_0_2_1 * p_speed_dist_0_2 +
      b_p_speed_dist_2_4_1 * p_speed_dist_2_4 +
      b_p_speed_dist_4_15_1 * p_speed_dist_4_15 +
      b_p_speed_dist_15_40_1 * p_speed_dist_15_40 +
      b_p_speed_dist_40_plus_1 * p_speed_dist_40_plus +
      
      b_p_speed_time_0_2_1 * p_speed_time_0_2 + 
      # b_p_speed_time_2_4_1 * p_speed_time_2_4 + 
      # b_p_speed_time_4_15_1 * p_speed_time_4_15 +
      # b_p_speed_time_15_40_1 * p_speed_time_15_40 + 
      # b_p_speed_time_40_plus_1 * p_speed_time_40_plus +
      
      b_distance_1 * distance_for_walk + 
      b_distance_log_1 * distance_for_walk_log +
      b_distance_greater_than_3 * distance_greater_than_3 +
      b_has_disability_1 * has_disability +
      b_has_disability_pnta_1 * has_disability_pnta +
      b_has_disability_missing_1 * has_disability_missing +
      b_weekday_1 * weekday_trip +
      b_arrive_before_6_1 * arrive_before_6 +
      b_arrive_7thru9_1 * arrive_7thru9 +
      b_arrive_16thru19_1 * arrive_16thru19 +
      b_arrive_after_20_1 * arrive_after_20 +
      
      b_d_county_fips_001_1 * d_county_fips_001 + 
      b_d_county_fips_075_1 * d_county_fips_075 +
      b_d_county_fips_085_1 * d_county_fips_085 +
      b_d_county_fips_037_1 * d_county_fips_037 +
    
      b_prev_inertia_1 * prev_inertia_1 +
      b_prev_inertia_home_1 * prev_inertia_1 * o_from_home +
      b_prev_inertia_change_mode_1 * prev_inertia_1 * o_change_mode +
      b_next_inertia_1 * next_inertia_1 +
      b_next_inertia_home_1 * next_inertia_1 * d_going_home +
      b_next_inertia_change_mode_1 * next_inertia_1 * d_change_mode +       
      b_close_to_home_1_1 * close_to_home_1 + b_close_to_home_2_1 * close_to_home_2 +
      b_close_to_work_1_1 * close_to_work_1 + b_close_to_work_2_1 * close_to_work_2 +
      b_perc_trips_2 * perc_walk_trips_2 + b_perc_trips_3 * perc_walk_trips_3 + b_perc_trips_4 * perc_walk_trips_4 +
      b_exercise_1 * d_exercising +
      b_unlinked_1 * unlinked + b_unlinked_split_1 * unlinked_split +
      b_unlinked_o_home_1 * unlinked * o_from_home +
      b_unlinked_d_home_1 * unlinked * d_going_home +
      b_split_loop_1 * split_due_to_loop +
      b_o_escort_1 * o_escort + b_d_escort_1 * d_escort +
      b_has_3_points_or_fewer_1 * has_3_points_or_fewer
    
    V[['bike']] = 
      asc_2 + 
      b_added_trip_2 * added_trip +
      b_age67_2 * age_67 + b_age8plus_2 * age_8plus + 
      b_income_2 * income + b_log_income_2 * income_log +
      b_speed_2 * median_speed_mph + 
      b_speed_log_2 * median_speed_mph_log + 
      b_speed_15_2 * speed_15plus +
      
      # b_p_speed_dist_0_2_2 * p_speed_dist_0_2 +
      b_p_speed_dist_2_4_2 * p_speed_dist_2_4 +
      b_p_speed_dist_4_15_2 * p_speed_dist_4_15 +
      b_p_speed_dist_15_40_2 * p_speed_dist_15_40 +
      b_p_speed_dist_40_plus_2 * p_speed_dist_40_plus +
      
      b_p_speed_time_0_2_2* p_speed_time_0_2 + 
      # b_p_speed_time_2_4_2* p_speed_time_2_4 + 
      # b_p_speed_time_4_15_2* p_speed_time_4_15 +
      # b_p_speed_time_15_40_2* p_speed_time_15_40 + 
      # b_p_speed_time_40_plus_2* p_speed_time_40_plus +
      
      b_distance_2 * distance + 
      b_distance_log_2 * distance_log +
      b_has_disability_2 * has_disability +
      b_has_disability_pnta_2 * has_disability_pnta +
      b_has_disability_missing_2 * has_disability_missing +
      b_weekday_2 * weekday_trip +
      b_arrive_before_6_2 * arrive_before_6 +
      b_arrive_7thru9_2 * arrive_7thru9 +
      b_arrive_16thru19_2 * arrive_16thru19 +
      b_arrive_after_20_2 * arrive_after_20 +
      
      b_d_county_fips_001_2 * d_county_fips_001 +
      b_d_county_fips_075_2 * d_county_fips_075 +
      b_d_county_fips_085_2 * d_county_fips_085 +
      b_d_county_fips_037_2 * d_county_fips_037 +
      
      b_prev_inertia_2 * prev_inertia_2 +
      b_prev_inertia_home_2 * prev_inertia_2 * o_from_home +
      b_prev_inertia_change_mode_2 * prev_inertia_2 * o_change_mode +
      b_next_inertia_2 * next_inertia_2 +
      b_next_inertia_home_2 * next_inertia_2 * d_going_home +            
      b_next_inertia_change_mode_2 * next_inertia_2 * d_change_mode +       
      b_close_to_home_1_2 * close_to_home_1 + b_close_to_home_2_2 * close_to_home_2 +
      b_close_to_work_1_2 * close_to_work_1 + b_close_to_work_2_2 * close_to_work_2 +
      b_perc_trips_2 * perc_bike_trips_2 + b_perc_trips_3 * perc_bike_trips_3 + b_perc_trips_4 * perc_bike_trips_4 +
      b_exercise_2 * d_exercising  +
      b_split_loop_2 * split_due_to_loop +
      b_o_escort_2 * o_escort + b_d_escort_2 * d_escort +
      b_has_3_points_or_fewer_2 * has_3_points_or_fewer
    
    V[['car']] =  
      b_has_license * has_license + b_missing_license * missing_license +
      b_has_vehicle_1 * has_vehicle_1 + b_has_vehicle_2 * has_vehicle_2 + b_has_vehicle_3 * has_vehicle_3 + 
      b_shopping_3 * d_shopping +
      b_o_got_gas_3 * o_got_gas + b_d_got_gas_3 * d_got_gas +
      b_prev_inertia_3 * prev_inertia_3 +
      b_prev_inertia_home_3 * prev_inertia_3 * o_from_home +
      b_prev_inertia_change_mode_3 * prev_inertia_3 * o_change_mode +
      b_next_inertia_3 * next_inertia_3 +
      b_next_inertia_home_3 * next_inertia_3 * d_going_home +
      b_next_inertia_change_mode_3 * next_inertia_3 * d_change_mode +
      b_speed_lt3_3 * speed_lt3 +
      b_speed_lt3_non_imp_3 * speed_lt3_non_imp
    
    # taxi (dropped)
    
    V[['transit']] =  
      asc_4 + 
      b_added_trip_4 * added_trip +
      b_age67_4 * age_67 + b_age8plus_4 * age_8plus + 
      b_income_4 * income + b_log_income_4 * income_log + 
      b_speed_4 * median_speed_mph + 
      b_speed_log_4 * median_speed_mph_log + 
      
      # b_p_speed_dist_0_2_4 * p_speed_dist_0_2 +
      b_p_speed_dist_2_4_4 * p_speed_dist_2_4 +
      b_p_speed_dist_4_15_4 * p_speed_dist_4_15 +
      b_p_speed_dist_15_40_4 * p_speed_dist_15_40 +
      b_p_speed_dist_40_plus_4 * p_speed_dist_40_plus +
      
      b_p_speed_time_0_2_4* p_speed_time_0_2 + 
      # b_p_speed_time_2_4_4* p_speed_time_2_4 + 
      # b_p_speed_time_4_15_4* p_speed_time_4_15 +
      # b_p_speed_time_15_40_4* p_speed_time_15_40 + 
      # b_p_speed_time_40_plus_4* p_speed_time_40_plus +
      
      b_distance_4 * distance + 
      b_distance_log_4 * distance_log +
      b_has_disability_4 * has_disability +
      b_has_disability_pnta_4 * has_disability_pnta +
      b_has_disability_missing_4 * has_disability_missing +
      b_weekday_4 * weekday_trip +
      b_arrive_before_6_4 * arrive_before_6 +
      b_arrive_7thru9_4 * arrive_7thru9 +
      b_arrive_16thru19_4 * arrive_16thru19 +
      b_arrive_after_20_4 * arrive_after_20 +
      b_transit_weekly_4plus * transit_weekly_4plus + b_transit_weekly_1to3 * transit_weekly_1to3 + b_transit_monthly * transit_monthly +

      b_d_county_fips_001_4 * d_county_fips_001 + 
      b_d_county_fips_075_4 * d_county_fips_075 +
      b_d_county_fips_085_4 * d_county_fips_085 +
      b_d_county_fips_037_4 * d_county_fips_037 +
      
      b_prev_inertia_4 * prev_inertia_4 +
      b_prev_inertia_home_4 * prev_inertia_4 * o_from_home +
      b_prev_inertia_change_mode_4 * prev_inertia_4 * o_change_mode +
      b_next_inertia_4 * next_inertia_4 +
      b_next_inertia_home_4 * next_inertia_4 * d_going_home +  
      b_next_inertia_change_mode_4 * next_inertia_4 * d_change_mode +       
      b_close_to_home_1_4 * close_to_home_1 + b_close_to_home_2_4 * close_to_home_2 +
      b_close_to_work_1_4 * close_to_work_1 + b_close_to_work_2_4 * close_to_work_2 +
      b_perc_trips_2 * perc_transit_trips_2 + b_perc_trips_3 * perc_transit_trips_3 + b_perc_trips_4 * perc_transit_trips_4  +
      b_unlinked_4 * unlinked + b_unlinked_split_4 * unlinked_split +
      b_o_escort_4 * o_escort + b_d_escort_4 * d_escort +
      b_has_3_points_or_fewer_4 * has_3_points_or_fewer
    
    # school bus (dropped)
    
    # other (dropped)
    
    # shuttle (dropped)
    
    V[['tnc']] =  
      asc_5 + 
      b_added_trip_5 * added_trip +
      b_age67_5 * age_67 + b_age8plus_5 * age_8plus + 
      b_income_5 * income + b_log_income_5 * income_log + 
      
      b_speed_5 * median_speed_mph + 
      b_speed_log_5 * median_speed_mph_log +
      
      # b_p_speed_dist_0_2_5 * p_speed_dist_0_2 +
      b_p_speed_dist_2_4_5 * p_speed_dist_2_4 +
      b_p_speed_dist_4_15_5 * p_speed_dist_4_15 +
      b_p_speed_dist_15_40_5 * p_speed_dist_15_40 +
      b_p_speed_dist_40_plus_5 * p_speed_dist_40_plus +
      
      b_p_speed_time_0_2_5* p_speed_time_0_2 + 
      # b_p_speed_time_2_4_5* p_speed_time_2_4 + 
      # b_p_speed_time_4_15_5* p_speed_time_4_15 +
      # b_p_speed_time_15_40_5* p_speed_time_15_40 + 
      # b_p_speed_time_40_plus_5* p_speed_time_40_plus +
      
      b_distance_5 * distance + 
      b_distance_log_5 * distance_log +
      b_has_disability_5 * has_disability +
      b_has_disability_pnta_5 * has_disability_pnta +
      b_has_disability_missing_5 * has_disability_missing +
      
      b_weekday_5 * weekday_trip +
      b_friday_5 * friday_trip +
      b_saturday_5 * saturday_trip +
      
      b_arrive_before_6_5 * arrive_before_6 +
      b_arrive_7thru9_5 * arrive_7thru9 +
      b_arrive_16thru19_5 * arrive_16thru19 +
      b_arrive_after_20_5 * arrive_after_20 +
      
      b_tnc_weekly_4plus * tnc_weekly_4plus + 
      b_tnc_weekly_1to3 * tnc_weekly_1to3 + 
      b_tnc_monthly * tnc_monthly +
      
      b_d_county_fips_001_5 * d_county_fips_001 + 
      b_d_county_fips_075_5 * d_county_fips_075 +
      b_d_county_fips_085_5 * d_county_fips_085 +
      b_d_county_fips_037_5 * d_county_fips_037 +
      
      b_prev_inertia_5 * prev_inertia_5 +
      b_prev_inertia_home_5 * prev_inertia_5 * o_from_home +
      b_prev_inertia_change_mode_5 * prev_inertia_5 * o_change_mode +
      b_next_inertia_5 * next_inertia_5 +
      b_next_inertia_home_5 * next_inertia_5 * d_going_home +
      b_next_inertia_change_mode_5 * next_inertia_5 * d_change_mode +       
      b_close_to_home_1_5 * close_to_home_1 + b_close_to_home_2_5 * close_to_home_2 +
      b_close_to_work_1_5 * close_to_work_1 + b_close_to_work_2_5 * close_to_work_2 +
      b_perc_trips_2 * perc_tnc_trips_2 + b_perc_trips_3 * perc_tnc_trips_3 + b_perc_trips_4 * perc_tnc_trips_4 +
      b_social_5 * d_social +
      b_work_related_5 * d_work_related +
      b_o_escort_5 * o_escort + b_d_escort_5 * d_escort +
      b_has_3_points_or_fewer_5 * has_3_points_or_fewer
    
    ### Define settings for MNL model component
    mnl_settings = list(
      alternatives = c(
        walk = 1,
        bike = 2,
        car = 3,
        transit = 4,
        tnc = 5),
      avail = 
        list(
          walk = av_walk,
          bike = av_bike,
          car = av_car,
          transit = av_transit,
          tnc = av_tnc),
      choiceVar    = mode_type_condensed,
      V            = V
    )
    
    ### Compute probabilities using MNL model
    P[["model"]] = apollo_mnl(mnl_settings, functionality)
    
    ### Take product across observation for same individual
    P = apollo_panelProd(P, apollo_inputs, functionality)
    
    ### Prepare and return outputs of function
    P = apollo_prepareProb(P, apollo_inputs, functionality)
    
    return(P)
  }


# Set param names (must match lik_fun) =========================================

param_names = c(
  "asc_1", "asc_2", "asc_4", "asc_5",
  
  "b_added_trip_1", "b_added_trip_2", "b_added_trip_4", "b_added_trip_5",
  
  "b_has_license", "b_missing_license",
  
  "b_speed_1", "b_speed_2", "b_speed_4", "b_speed_5",
  "b_speed_log_1", "b_speed_log_2", "b_speed_log_4", "b_speed_log_5",
  "b_speed_05_1", "b_speed_15_2", "b_speed_lt3_3", "b_speed_lt3_non_imp_3",
  
  "b_speed_3points_1", "b_speed_3points_2", "b_speed_3points_4", "b_speed_3points_5",
  "b_speed_log_3points_1", "b_speed_log_3points_2", "b_speed_log_3points_4", "b_speed_log_3points_5",
  "b_speed_05_3points_1", "b_speed_15_3points_2", "b_speed_lt3_ni_3points_3",
  
  # "b_p_speed_dist_0_2_1", "b_p_speed_dist_0_2_2", "b_p_speed_dist_0_2_4", "b_p_speed_dist_0_2_5",
  "b_p_speed_dist_2_4_1", "b_p_speed_dist_2_4_2", "b_p_speed_dist_2_4_4", "b_p_speed_dist_2_4_5",
  "b_p_speed_dist_4_15_1", "b_p_speed_dist_4_15_2", "b_p_speed_dist_4_15_4", "b_p_speed_dist_4_15_5",
  "b_p_speed_dist_15_40_1", "b_p_speed_dist_15_40_2", "b_p_speed_dist_15_40_4", "b_p_speed_dist_15_40_5",
  "b_p_speed_dist_40_plus_1", "b_p_speed_dist_40_plus_2", "b_p_speed_dist_40_plus_4", "b_p_speed_dist_40_plus_5",
  
  "b_p_speed_time_0_2_1", "b_p_speed_time_0_2_2", "b_p_speed_time_0_2_4", "b_p_speed_time_0_2_5",
  # "b_p_speed_time_2_4_1", "b_p_speed_time_2_4_2", "b_p_speed_time_2_4_4", "b_p_speed_time_2_4_5",
  # "b_p_speed_time_4_15_1", "b_p_speed_time_4_15_2", "b_p_speed_time_4_15_4", "b_p_speed_time_4_15_5",
  # "b_p_speed_time_15_40_1", "b_p_speed_time_15_40_2", "b_p_speed_time_15_40_4", "b_p_speed_time_15_40_5",
  # "b_p_speed_time_40_plus_1", "b_p_speed_time_40_plus_2", "b_p_speed_time_40_plus_4", "b_p_speed_time_40_plus_5",
  
  "b_has_vehicle_1", "b_has_vehicle_2", "b_has_vehicle_3",
  
  "b_distance_1", "b_distance_2", "b_distance_4", "b_distance_5",
  "b_distance_log_1", "b_distance_log_2", "b_distance_log_4", "b_distance_log_5",
  
  "b_has_disability_1", "b_has_disability_pnta_1", "b_has_disability_missing_1",
  "b_has_disability_2", "b_has_disability_pnta_2", "b_has_disability_missing_2",
  "b_has_disability_4", "b_has_disability_pnta_4", "b_has_disability_missing_4",
  "b_has_disability_5", "b_has_disability_pnta_5", "b_has_disability_missing_5",
  
  "b_weekday_1", "b_weekday_2", "b_weekday_4", "b_weekday_5",  
  "b_friday_5", "b_saturday_5",
  
  "b_age67_1", "b_age67_2", "b_age67_4", "b_age67_5",
  "b_age8plus_1", "b_age8plus_2", "b_age8plus_4", "b_age8plus_5",
  
  "b_arrive_before_6_1", "b_arrive_7thru9_1", "b_arrive_16thru19_1", "b_arrive_after_20_1",
  "b_arrive_before_6_2", "b_arrive_7thru9_2", "b_arrive_16thru19_2", "b_arrive_after_20_2",
  "b_arrive_before_6_4", "b_arrive_7thru9_4", "b_arrive_16thru19_4", "b_arrive_after_20_4",
  "b_arrive_before_6_5", "b_arrive_7thru9_5", "b_arrive_16thru19_5", "b_arrive_after_20_5",  
  
  "b_income_1", "b_income_2", "b_income_4", "b_income_5",  
  "b_log_income_1", "b_log_income_2", "b_log_income_4", "b_log_income_5",  
  
  "b_transit_weekly_4plus", "b_transit_weekly_1to3", "b_transit_monthly",
  "b_tnc_weekly_4plus", "b_tnc_weekly_1to3", "b_tnc_monthly",
  
  "b_d_county_fips_001_1","b_d_county_fips_001_2","b_d_county_fips_001_4","b_d_county_fips_001_5",
  "b_d_county_fips_075_1","b_d_county_fips_075_2","b_d_county_fips_075_4","b_d_county_fips_075_5",
  "b_d_county_fips_085_1","b_d_county_fips_085_2","b_d_county_fips_085_4","b_d_county_fips_085_5",
  "b_d_county_fips_037_1","b_d_county_fips_037_2","b_d_county_fips_037_4","b_d_county_fips_037_5",
  
  "b_prev_inertia_1", "b_prev_inertia_2", "b_prev_inertia_3", "b_prev_inertia_4", "b_prev_inertia_5",
  "b_prev_inertia_home_1", "b_prev_inertia_home_2", "b_prev_inertia_home_3", "b_prev_inertia_home_4", "b_prev_inertia_home_5",
  "b_prev_inertia_change_mode_1", "b_prev_inertia_change_mode_2", "b_prev_inertia_change_mode_3", "b_prev_inertia_change_mode_4", "b_prev_inertia_change_mode_5", 
  
  "b_next_inertia_1", "b_next_inertia_2", "b_next_inertia_3", "b_next_inertia_4", "b_next_inertia_5",
  "b_next_inertia_home_1", "b_next_inertia_home_2", "b_next_inertia_home_3", "b_next_inertia_home_4", "b_next_inertia_home_5",  
  "b_next_inertia_change_mode_1", "b_next_inertia_change_mode_2", "b_next_inertia_change_mode_3", "b_next_inertia_change_mode_4", "b_next_inertia_change_mode_5", 
  
  "b_close_to_home_1_1", "b_close_to_home_2_1",
  "b_close_to_home_1_2", "b_close_to_home_2_2",
  "b_close_to_home_1_4", "b_close_to_home_2_4",
  "b_close_to_home_1_5", "b_close_to_home_2_5",
  
  "b_close_to_work_1_1", "b_close_to_work_2_1",
  "b_close_to_work_1_2", "b_close_to_work_2_2",
  "b_close_to_work_1_4", "b_close_to_work_2_4",
  "b_close_to_work_1_5", "b_close_to_work_2_5",
  
  "b_perc_trips_2", "b_perc_trips_3", "b_perc_trips_4",
  
  "b_shopping_3", "b_exercise_1", "b_exercise_2",
  "b_social_5", "b_work_related_5",
  
  "b_unlinked_1", "b_unlinked_4", 
  "b_unlinked_split_1", "b_unlinked_split_4",
  "b_unlinked_o_home_1", "b_unlinked_d_home_1",
  
  "b_split_loop_1", "b_split_loop_2",
  
  "b_o_escort_1", "b_o_escort_2", "b_o_escort_4", "b_o_escort_5",
  "b_d_escort_1", "b_d_escort_2", "b_d_escort_4", "b_d_escort_5",
  
  "b_has_3_points_or_fewer_1", "b_has_3_points_or_fewer_2", "b_has_3_points_or_fewer_4", "b_has_3_points_or_fewer_5",
  
  "b_o_got_gas_3", "b_d_got_gas_3",
  
  "b_distance_greater_than_3"
  
)
