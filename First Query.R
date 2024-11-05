library('dplyr')
library('httr2')
library('stringr')
library('purrr')

# Constants

domain <- 'https://llb.piwik.pro'

event_types <- 'https://developers.piwik.pro/en/latest/_downloads/c40e53f6d91c60b0645fe718b696a435/event_type.json'

## Credentials

creds <- list(
  client_id = 'h1PZI3KuScC5WN1V52PXVl6OCsvZu61F',
  client_secret = 'mjCyyIE9C5eqoWhmK7DYJJLcKHLX1Ql87a3XocR3t8JkVJEBlssMrKTXSnZsr3E1HO6DyalxNNM87S0q',
  website_id = '43eb9f91-5926-45c0-ba1d-b346359141b6'
)

## Endpoints

endpoints <- list(
  query = '/api/analytics/v1/query/',
  session = '/api/analytics/v1/sessions/',
  event = '/api/analytics/v1/events/',
  auth = '/auth/token'
)

## Pagination Variables

offset <- 0
limit <- 1000

# Authentication

json_auth <- list('grant_type' = 'client_credentials', 
                  'client_id' = creds$client_id, 
                  'client_secret' = creds$client_secret)

auth_req <- request(paste0(domain, endpoints$auth))

auth_resp <- auth_req %>% req_body_json(json_auth) %>% req_perform()

token <- auth_resp %>% resp_body_json()

# Generate Columns

input_cols  <- tribble(
    ~column, ~transformation,
    'timestamp', 'to_date',
    'referrer_type', '',
    'source_medium', '',
    'campaign_name', '',
    'sessions', '',
    'bounces', ''
)

col_list <- function(input) {
  return_list <- list('column_id' = input[['column']])
  if (input[['transformation']] != '') {
    return_list[['transformation_id']] <- input[['transformation']]
  }
  return(return_list)
}

# Build list for columns
columns <- apply(input_cols, 1, col_list)

  
# Generate Filter

## ToDo: Find way to integrate nested filter conditions

input_filter <- tribble(
  ~column, ~operator, ~value,
  'event_url', 'contains', 'karriere'
)

cond_list <- function(input) {
  return_list = list(
    'column_id' = input[['column']],
    'condition' = list (
      'operator' = input[['operator']],
      'value' = input[['value']]
    )
  )
  
  return(return_list)
}

conds <- apply(input_filter, 1, cond_list)

global_operator <- 'and'

filters <- list(
  'operator' = global_operator,
  'conditions' = conds
)
  
filters_example <- list(
  'operator' = 'and',
  'conditions' = list( list(
    'operator' = 'or',
    'conditions' = list(
      list(
        'column_id' = 'event_url',
        'condition' = list (
          'operator' = 'contains',
          'value' = 'karriere'
        )
      ),
      list(
        'operator' = 'and',
        'conditions' = list( list(
        'column_id' = 'event_url',
        'condition' = list (
          'operator' = 'contains',
          'value' = 'query'
        )
        ))
      )
    )
  ))
)
  
# Generate Query

today <- Sys.Date()

query <- list('date_from' = '2023-01-01',
              'date_to' = today,
              'website_id' = creds$website_id,
              'offset' = offset,
              'limit' = limit,
              'columns' = columns
              #'order_by' = list(list(2, 'desc')),
              #'filters' = filters_example
              #'metric_filters' = NULL
              )

## Set Query Endpoint

query_req <- request(paste0(domain, endpoints$query))

## Set Query Error

error_body <- function(query_resp) {
  resp_body_json(query_resp)$error
}

# Execute Query

run <- TRUE
rm(result)

while (run == TRUE) {
  
  query_resp <- query_req %>%
    req_auth_bearer_token(token$access_token) %>% 
    req_headers('Content-Type' = "application/vnd.api+json") %>% 
    req_body_json(query) %>% 
    req_error(body = error_body) %>% 
    req_verbose() %>%
    req_perform(verbosity = 3)
  
  data_json <- query_resp %>% resp_body_json()
  
  if (length(data_json$data) > 0) {
    
    run <- TRUE
  
    # Transform to Data Frame
    
    data <- data_json[["data"]]
    cols <- data_json[["meta"]][["columns"]]
    
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

result_new <- result

result_new$event_type <- lapply(result_new$event_type, '[[', 2)

result_new$event_url <- str_replace(result_new$event_url, 'www.', '')

df <- as.data.frame(sapply(result_new, unlist))

write.csv(df, file = '20240611 MDH - Piwik Pro.csv', row.names = FALSE)


