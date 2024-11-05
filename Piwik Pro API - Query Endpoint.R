library(tidyverse)
library(httr2)

#Constants
domain <- 'https://llb.piwik.pro'

website_id = list(
  llb_group_prod = 'd048caa7-e245-42ab-a237-6661ce35cba8',
  willbe_web = '567700c8-8541-4312-8c68-a1dbbd0e5c24',
  willbe_app = 'fcad831e-2c95-4ad9-ad6b-47ff6df47772',
  willbe_all = 'cdc724c6-3fe6-4c26-944e-a15e9cee78ad',
  fl_web = '43eb9f91-5926-45c0-ba1d-b346359141b6',
  ch_web = '376eadd4-5818-4801-9b81-cb7e7489e4bc',
  at_web = 'f5750686-3665-47c3-8804-227880cecef1'
)

pp_id <- website_id$willbe_all

today <- Sys.Date()
start_date <- '2023-04-20'
end_date <- '2023-04-24'


##Credentials
creds <- list(
  client_id = 'h1PZI3KuScC5WN1V52PXVl6OCsvZu61F',
  client_secret = 'mjCyyIE9C5eqoWhmK7DYJJLcKHLX1Ql87a3XocR3t8JkVJEBlssMrKTXSnZsr3E1HO6DyalxNNM87S0q'
)

##Endpoints
endpoints <- list(
  query = '/api/analytics/v1/query/',
  session = '/api/analytics/v1/sessions/',
  event = '/api/analytics/v1/events/',
  auth = '/auth/token'
)

##Pagination variables
offset <- 0
limit <- 10000

##Create dictionary for column renaming
nested_cols <- list(
  website_name = c('app_id', 'app_name'),
  operating_system = c('os_code', 'os_name'),
  browser_name = c('browser_code', 'browser_name'),
  browser_language_iso639 = c('browser_lang_code', 'browser_lang'),
  device_brand = c('device_brand_code', 'device_brand'),
  location_continent_iso_code = c('location_continent_code', 'location_continent_name'),
  location_country_name = c('location_country_code', 'location_country_name'),
  location_subdivision_1_name = c('location_subdivision_code', 'location_subdivision_name'),
  location_city_name = c('location_city_code', 'location_city_name')
)

#Helper functions

##Authentication
auth_token <- function () {
  
  json_auth <- list('grant_type' = 'client_credentials', 
                    'client_id' = creds$client_id, 
                    'client_secret' = creds$client_secret)
  
  auth_req <- request(paste0(domain, endpoints$auth))
  
  auth_resp <- auth_req %>% req_body_json(json_auth) %>% req_perform()
  
  token <<- auth_resp %>% resp_body_json()
  
}

##Column list
col_list <- function(input) {
  return_list <- list('column_id' = input[['column']])
  if (input[['transformation']] != '') {
    return_list[['transformation_id']] <- input[['transformation']]
  }
  return(return_list)
}

##Replace NULL in list-columns
replace_null <- function(x) {
  if (is.list(x)) {
    return(map(x, replace_null))  # Recursively apply to each list element
  } else {
    return(ifelse(is.null(x), NA, x))  # Replace NULL with NA (or any value)
  }
}

#Define columns & filters
queries <- list(
  test_query = list(
    cols = tribble(
      ~column, ~transformation,
      'website_name', '',
      'visitor_returning', '',
      'referrer_type', '',
      'operating_system', '',
      'browser_name', '',
      'browser_language_iso639', '',
      'device_type', '', 
      'device_brand', '',
      'source_medium', '',
      'location_continent_iso_code', '',
      'location_country_name', '',
      'location_subdivision_1_name', '',
      'location_subdivision_2_name', '',
      'location_city_name', '',
      'location_metro_code', '',
      'event_type', '',
      'consent_source', '',
      'consent_form_button', '',
      'consent_scope', '',
      'consent_action', '',
      'events', ''
    ),
    filter = list(
      'operator' = 'and',
      'conditions' = list( list(
        'operator' = 'or',
        'conditions' = list(
          list(
            'column_id' = 'session_entry_url',
            'condition' = list (
              'operator' = 'matches',
              'value' = '.*'
            )
          )
        )
      ))
    )
  )
)

#Run queries
for (i in names(queries)) {
  print(i)
  print(queries[[i]]$cols)
  
  #Define cols and filters
  assign(paste(i, '_cols', sep = ''), apply(queries[[i]]$cols, 1, col_list))
  assign(paste(i, '_filter', sep = ''), queries[[i]]$filter)
  
  #Generate query
  query <- list('date_from' = start_date,
                'date_to' = end_date,
                'website_id' = pp_id,
                'offset' = offset,
                'limit' = limit,
                'columns' = apply(queries[[i]]$cols, 1, col_list),
                #'order_by' = list(list(2, 'desc')),
                'filters' = queries[[i]]$filter
                #'metric_filters' = NULL
  )
  
  #Set query endpoint
  query_req <- request(paste0(domain, endpoints$query))
  
  #Set query error
  error_body <- function(query_resp) {
    resp_body_json(query_resp)$error
  }
  
  #Execute query
  
  ##Generate auth token
  auth_token()
  
  ##Reset while variables
  run <- TRUE
  if (exists('result') == TRUE) {
    rm(result)
  }
  
  ##Execute all queries
  while (run == TRUE) {
    
    query_resp <- query_req %>%
      req_auth_bearer_token(token$access_token) %>% 
      req_headers('Content-Type' = 'application/vnd.api+json') %>% 
      req_body_json(query) %>% 
      req_error(body = error_body) %>% 
      req_verbose() %>%
      req_perform(verbosity = 3)
    
    data_json <- query_resp %>% resp_body_json()
    
    if (length(data_json$data) > 0) {
      
      run <- TRUE
      
      ###Extract list from response
      
      data <- data_json[['data']]
      cols <- data_json[['meta']][['columns']]
      
      print(exists('result'))
      
      if (exists('result') == FALSE) { 
        result <- data.frame(matrix(nrow = 0, ncol = length(cols)))
        colnames(result) <- cols
      }
      
      data <- as.data.frame(t(do.call(cbind, data)))
      colnames(data) <- cols
      
      result <- rbind(result, data)
      
      print(query$offset)
      print(length(data_json$data))
      
      query$offset <- query$offset + limit
    } else {
      run <- FALSE
      query$offset <- offset
      break
    }
    
  }
  
  #Convert to dataframe
  result <- as.data.frame(result)
  
  #Replace all NULL values with NA
  result <- result %>%
    mutate(across(everything(), replace_null))
  
  #Unnest columns
  for (col in names(result)) {
    #Unnest list-columns
    if (length(result[[col]][[1]]) > 1) { 
      #If column names are specified in nested_col, apply naming and extract both values
      if (col %in% names(nested_cols)) {
        for (item in seq_along(1:length(result[[col]][[1]]))) {
          col_name <- nested_cols[[col]][item]
          result[[col_name]] <- sapply(result[[col]], '[[', item)
          result <- result %>% relocate(all_of(col_name), .after = all_of(col))
        }
      } else {
        #If column names are not specified, use existing name and extract second value
        result[[col]] <- sapply(result[[col]], '[[', 2)
      }
    } else {
      #Unlist non-list-columns
      result[[col]] <- unlist(result[[col]])
    }
  }
  
  #Select only flat columns
  df <- df %>% select(!where(is.list))
  
  #Google Ads campaign names
  if ('google_ads_campaign_name' %in% names(df)) {
    df <- df %>%
      mutate(
        campaign_name = case_when(
          campaign_name == 'google ads unknown' ~ google_ads_campaign_name,
          .default = campaign_name
        )
      )
  }
  
  #Teads campaigns from query parameter event_url
  if ('event_url' %in% names(df)) {
    df <- df %>% 
      mutate(
        campaign_name = case_when(
          grepl('auctid', event_url) ~ 'teads campaign',
          .default = campaign_name
        )
      )
  }
  
  #Teads campaigns from query parameter session_entry_url
  if ('session_entry_url' %in% names(df)) {
    df <- df %>% 
      mutate(
        campaign_name = case_when(
          grepl('auctid', session_entry_url) ~ 'teads campaign',
          .default = campaign_name
        )
      )
  }
  
  #Job ID
  if ('outlink_url' %in% names(df)) {
    df <- df %>%
      mutate(
        job_id = str_split_i(outlink_url, '/', 5),
        .before = last_col(offset = 1L)
      )
  }
  
  #Query parameter
  if ('event_url' %in% names(df)) {
    df <- df %>%
      mutate(
        query_parameter = str_split_i(event_url, '\\?', 2), 
        .before = last_col(offset = 1L)
      )
  }
  
  #Campaign ID
  if ('campaign_id' %in% names(df)) {
    df <- df %>%
      mutate(
        campaign_id_source = campaign_id,
        campaign_id = str_split_i(campaign_id_source, '-', 1),
        ad_group_id = str_split_i(campaign_id_source, '-', 2),
        ad_id = str_split_i(campaign_id_source, '-', 3),
        .after = campaign_name
      )
  }
  
  #String replace
  df <- df %>%
    mutate(across(!where(~is.numeric(.x)), .fns = ~str_replace_all(.x, 'www.|wwt.', ''))) %>%
    mutate(across(!where(~is.numeric(.x)), .fns = ~str_split_i(.x, '\\?', 1))) %>%
    mutate(across(!where(~is.numeric(.x)), .fns = ~replace_na(.x, '')))
  
  #Value formatting
  df <- df %>%
    mutate(across(contains('date'), .fns = ~as.Date(.x, format = '%Y-%m-%d'))) %>%
    mutate(across(c(where(~any(str_detect(.x, '^[0-9]+$'))), -contains('campaign_'), -contains('ad_'), -contains('_code')), .fns = ~as.numeric(.x))) %>%
    mutate(across(c(contains('_code')), .fns = ~as.character(.x)))
  
  
  #Rename columns
  rename_cols <- c(
    date = 'timestamp__to_date',
    entry_url = 'session_entry_url',
    entry_url_domain = 'session_entry_url__to_domain',
    event_url_domain = 'event_url__to_domain'
  )
  
  df <- df %>%
    rename(any_of(rename_cols))
  
  #Drop columns
  drop_cols <- c('google_ads_campaign_name')
  
  df <- df %>%
    select(!any_of(drop_cols))
  
  #Name response
  assign(i, df)
}