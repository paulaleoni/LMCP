## make figures out of txt and pre/post billing data

library(tidyverse)
#library(grid)
library(gridExtra)

getCurrentFileLocation <-  function()
{
  this_file <- commandArgs() %>% 
    tibble::enframe(name = NULL) %>%
    tidyr::separate(col=value, into=c("key", "value"), sep="=", fill='right') %>%
    dplyr::filter(key == "--file") %>%
    dplyr::pull(value)
  if (length(this_file)==0)
  {
    this_file <- rstudioapi::getSourceEditorContext()$path
  }
  return(dirname(this_file))
}

wd <- getCurrentFileLocation()
wd.parent <- dirname(wd)

#### load data ####
path_txt <- file.path(wd.parent,'data','cons_data_all.zip')
df_txt <- read_delim(unz(path_txt,'cons_data_all.csv'), delim =',')

path_post <- file.path(wd.parent,'data','post_pre_paid.zip')
df_post <- read_delim(unz(path_post,'Postpaid_AFDB_TX_Data_20220126.txt'), delim='|')

path_pre <- file.path(wd.parent,'data','post_pre_paid.zip')
df_pre <- read_delim(unz(path_post,'Prepaid_AFDB_TX_Data_20220126.txt'), delim='|')

#### prepare data ####
# lowercase column names
names(df_txt) <- tolower(names(df_txt))
names(df_post) <- tolower(names(df_post))
names(df_pre) <- tolower(names(df_pre))

# change all to 'billing_date'
df_txt <- df_txt %>% rename(billing_date = vending_date)
df_pre <- df_pre %>% rename(billing_date = date_of_vend)

# change all to id_bill
df_pre <- df_pre %>% rename(id_bill = receipt_no)

# change all to installation_date
df_txt <- df_txt %>% rename(installation_date = meterinstdate)

# change all to collected 
df_txt <- df_txt %>% rename(collected = debt_collected)

# change all to meternumber
df_pre <- df_pre %>% rename(meternumber = serial_num)
df_post <- df_post %>% rename(meternumber = serial_num)

# make date to class "POSIXct"
df_txt$billing_date <- df_txt$billing_date %>% as.POSIXct()
df_pre$billing_date <- df_pre$billing_date %>% as.POSIXct()
df_post$billing_date <- df_post$billing_date %>% as.POSIXct()

df_txt$installation_date <- df_txt$installation_date %>% as.POSIXct()
df_pre$installation_date <- df_pre$installation_date %>% as.POSIXct()
df_post$installation_date <- df_post$installation_date %>% as.POSIXct()


group_df <- function(df){
  # add rows if same meternumber and billing_date
  df_group <- df %>% group_by(meternumber, billing_date) %>% 
    summarise(amount = sum(amount, na.rm=T), 
              units = sum(units, na.rm=T), 
              #collected = sum(collected, na.rm=T) 
              )
  df <- df[, !names(df) %in% c('amount','units','collected')] %>%
    distinct(.keep_all = TRUE) %>%
    merge(df_group, by=c('meternumber','billing_date'), all.x = T)
  # get first billing date per meter
  df_group <- df %>% group_by(meternumber) %>%
    summarise(first_billing_date = min(billing_date))
  df <- df %>% merge(df_group, by='meternumber', all.x=T)
  print('done')
  return(df)
}
  
modify_dates <- function(df){
  # extract date attribute
  weekdays <- c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
                "Sun")
  df['year'] <- df[['billing_date']] %>% format.POSIXct(format='%Y') %>% as.numeric()
  df['month'] <- df[['billing_date']] %>% format.POSIXct(format='%m') 
  df['yearmonth'] <- paste0(lapply(df[['year']], as.character), df[['month']])
  df['dayofmonth'] <- df[['billing_date']] %>% format.POSIXct(format='%d') %>% as.numeric()
  df['dayofweek'] <- as.POSIXlt(df[['billing_date']])$wday %>%
    factor(levels = c(0,1,2,3,4,5,6),
       labels = weekdays)
  ## connection categories
  # before July 2016
  # between July 2016 and June 2018
  # after June 2018
  cats <- c("before mid 2016","mid 2016 to mid 2018", "after mid 2018")
  df <- df %>% mutate(connection_cat = ifelse(installation_date < as.Date("2016-07-01"),
                                                      cats[1],
                                                      ifelse((as.Date("2016-07-01") <= installation_date) & 
                                                               (installation_date < as.Date("2018-07-01")) ,
                                                             cats[2],
                                                             ifelse(installation_date >= as.Date("2018-07-01"), cats[3],NA))))
  df$connection_cat <- df$connection_cat %>% factor(levels=cats,
                                                    labels=cats)
  print('done')
  return(df)
  }

df_txt <- df_txt %>% drop_na(billing_date) %>% group_df() %>% modify_dates()
df_post <- df_post %>% drop_na(billing_date) %>% group_df() %>% modify_dates()
df_pre <- df_pre %>% drop_na(billing_date) %>% group_df() %>% modify_dates()

# get months since connection
df_txt['months_since_installation'] <- difftime(df_txt$billing_date, df_txt$installation_date, units="weeks") %>% as.integer()
df_post['months_since_installation'] <- difftime(df_post$billing_date, df_post$installation_date, units="weeks") %>% as.integer()
df_pre['months_since_installation'] <- difftime(df_pre$billing_date, df_pre$installation_date, units="weeks") %>% as.integer()



#### figures ####
# set path to save figures
path_figures <-  file.path(wd.parent,'figures','Rfigures')

list_df <- list(df_txt, df_post, df_pre)
list_df_names <- list('txt','post','pre')

# percentage of purchases per day of month and weekday
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  # percentage purchases day of month
  plt <- df %>% ggplot(aes(dayofmonth)) + geom_bar(aes(y = (..count..)/sum(..count..))) +
    scale_y_continuous(labels=scales::percent) +
    theme_minimal() + theme(text = element_text(size=6)) + ylab("") + xlab("") +
    labs(title="Day of Month", caption="percentage of purchases per day of month")
  path_plt <- file.path(path_figures, name_df, paste0(name_df,'_dayofmonth.png'))
  ggsave(path_plt, plt)
  # percentage purchases day of week
  plt <- df %>% ggplot(aes(dayofweek)) + geom_bar(aes(y = (..count..)/sum(..count..))) +
    scale_y_continuous(labels=scales::percent) +
    theme_minimal() + theme(text = element_text(size=6)) + ylab("") + xlab("") +
    labs(title="Day of Week", caption="percentage of purchases per weekday")
  path_plt <- file.path(path_figures, name_df,paste0(name_df,'_dayofweek.png'))
  ggsave(path_plt, plt)
}

# histograms of monthly usage
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  # group data
  df <- df %>% group_by(meternumber, yearmonth) %>% # total amount per meternumber each yearmonth
    summarise(units = sum(units, na.rm=T),
              amount = sum(amount, na.rm=T))
  df <- df %>% group_by(meternumber) %>% 
    summarise(units = mean(units, na.rm=T), # average yearmonth amount per meternumber
              amount = mean(amount, na.rm=T))
  # monthly units lower 99%
  plt <- df %>% filter(units <= quantile(df[['units']],probs=c(0.99))) %>% 
    ggplot(aes(x=units)) + 
    geom_histogram(aes(y=..density..), color=1, fill = "white") + 
    geom_density(color='black') +
    theme_minimal() + theme(text = element_text(size=6)) + ylab("") + xlab("") +
    labs(title="Distribution of monthly usage", caption="distribution of average monthly units per meter - lower 99%")
  path_plt <- file.path(path_figures, name_df, paste0(name_df,'_distribution_units.png'))
  ggsave(path_plt, plt)
  # monthly amount lower 99%
  plt <- df %>% filter(amount <= quantile(df[['amount']],probs=c(0.99))) %>% 
    ggplot(aes(x=amount)) + 
    geom_histogram(aes(y=..density..), color=1, fill = "white") + 
    geom_density(color='black') +
    theme_minimal() + theme(text = element_text(size=6)) + ylab("") + xlab("") +
    labs(title="Distribution of monthly usage", caption="distribution of average monthly amount per meter - lower 99%")
  path_plt <- file.path(path_figures, name_df,paste0(name_df,'_distribution_amount.png'))
  ggsave(path_plt, plt)
}

# time series plots
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  df <- df %>% group_by(yearmonth, meternumber) %>% 
    summarise(units = sum(units, na.rm=T),
              amount = sum(amount, na.rm=T)) 
  df[['yearmonth']] <- df[['yearmonth']] %>% paste0('01') %>% as.Date(format='%Y%m%d')
  color_units <- "blue"
  color_amount <- "violet"
  scale_units <- 100
  scale_amount <- 100
  # plot units
  Nov18 <- geom_vline(xintercept= as.numeric(as.Date("2018-11-01")), linetype="dashed")
  p1 <- df %>% ggplot(aes(x=yearmonth, y = units / scale_units)) +
    geom_smooth(stat = 'summary', alpha = 0.2, fill = color_units, color = color_units,
                fun.data = median_hilow, fun.args = list(conf.int = .5)) +
    Nov18 + 
    theme_minimal() + theme(axis.title.y.left = element_text(color = color_units),
                            text = element_text(size=5)) +
    xlab("") + ylab(paste("units in",scale_units, "KwH??"))
  # plot amount
  p2 <- df %>% ggplot(aes(x=yearmonth, y = amount / scale_amount)) +
    geom_smooth(stat = 'summary', alpha = 0.2, fill = color_amount, color = color_amount,
                fun.data = median_hilow, fun.args = list(conf.int = .5)) +
    Nov18 + 
    theme_minimal() + theme(axis.title.y.left = element_text(color = color_amount),
                            text = element_text(size=5)) +
    xlab("") + ylab(paste("amount in",scale_amount, "??")) + 
    labs(caption = "monthly median customer's consumption - IQR")
  grob <- arrangeGrob(p1,p2,nrow=2)
  plt <- grid.arrange(grob) 
  path_plt <- file.path(path_figures, name_df,paste0(name_df,'_ts.png'))
  ggsave(path_plt, plt)
}

# consumption since connection
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  # total consumption per meter each month
  df <- df %>% group_by(connection_cat, months_since_installation, meternumber)%>% # total amount per meternumber each month since connection
    summarise(units = sum(units, na.rm=T),
              amount = sum(amount, na.rm=T)) %>%
    group_by(connection_cat, months_since_installation) %>% 
    summarise(units = median(units, na.rm=T), # average consumption per months_since_connection per connection_cat
              amount = median(amount, na.rm=T))
  plt <- df %>% drop_na(connection_cat) %>% ggplot() + 
    geom_line(aes(x = months_since_installation, y = units, colour=connection_cat)) + 
    theme_minimal() + theme(text = element_text(size=6)) +
    ylab("units in KwH?") + xlab("months since connection date") + 
    labs(title = "median monthly meter consumption since connection", colour="connected")
  path_plt <- file.path(path_figures, name_df,paste0(name_df,'_cons_since_connection.png'))
  ggsave(path_plt, plt)
}

# #customers and total units per year
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  #customers
  plt <- df %>% group_by(year) %>%
    summarise(n_cust = n_distinct(meternumber)) %>%
    mutate(year = paste0(as.character(year),'01','01') %>% as.Date(format='%Y%m%d')) %>%
    ggplot() +
    geom_line(aes(x=year, y=n_cust)) +
    theme_minimal() + theme(text = element_text(size=6)) + xlab("") +
    ylab("total number of customers")
  path_plt <- file.path(path_figures, name_df, paste0(name_df,'_ts_ncust.png'))
  ggsave(path_plt, plt)
  # total units
  scale_units <- 100000
  plt <- df %>% group_by(year) %>%
    summarise(units = sum(units)) %>%
    mutate(year = paste0(as.character(year),'01','01') %>% as.Date(format='%Y%m%d')) %>%
    ggplot() +
    geom_line(aes(x=year, y=units/scale_units)) +
    theme_minimal() + theme(text = element_text(size=6)) + xlab("") +
    ylab(paste("total sum of units sold in", scale_units,"KwH?"))
  path_plt <- file.path(path_figures, name_df, paste0(name_df,'_ts_totunits.png'))
  ggsave(path_plt, plt)
}

# #customers since connection
for(i in 1:length(list_df)){
  df <- list_df[[i]]
  name_df <- list_df_names[[i]]
  # total consumption per meter each month
  df <- df %>% group_by(months_since_installation, connection_cat) %>% 
    summarise(n_cust = n_distinct(meternumber))
  plt <- df %>% drop_na(connection_cat) %>% ggplot() + 
    geom_line(aes(x = months_since_installation, y = n_cust, colour=connection_cat)) +
    theme_minimal() + theme(text = element_text(size=6)) +
    ylab("monthly number of customers") + xlab("months since connection date") + 
    labs(colour="connected")
  path_plt <- file.path(path_figures, name_df, paste0(name_df,'_ncust_since_connection.png'))
  ggsave(path_plt, plt)
    
}


