# Load libraries

library(rpart)
library(rpart.plot)
library(randomForest)
library(tree)
library(sqldf)

#### Functions

# parse rpart tree into simple branch conditional statement logic

parse_tree <- function (df=NULL, model=NULL) {
  log <- capture.output({
    rpart.rules <- path.rpart(model,rownames(model$frame)[model$frame$var=="<leaf>"])
  })  
  
  args <- c("<=",">=","<",">","=")
  rules_out <- "case "
  i <- 1

  for (rule in rpart.rules) {  
    rule_out <- character(0)
    for (component in rule) {
      sep <- lapply(args, function(x) length(unlist(strsplit(component,x)))) > 1
      elements <- unlist(strsplit(component,(args[sep])[1]))
      if(!(elements[1]=="root")) {
        if (is.numeric(df[,elements[[1]]])) {
          rule_out <- c(rule_out,paste(elements[1],(args[sep])[1],elements[2]))
        } else {
          rule_out <- c(rule_out,paste0(elements[1]," in (",paste0("'",unlist(strsplit(elements[2],",")),"'",collapse=","),")"))
        }
      }
    }
    rules_out <- c(rules_out, paste0("when ", paste(rule_out,collapse=" AND ")," then 'node_" ,names(rpart.rules)[i],"'"))
    if(i==length(rpart.rules)) rules_out <- c(rules_out," end ")
    i <- i +1
  }
  sql_out <- paste(rules_out, collapse=" ")
  sql_out  
}

#########
## start code
########

# load product ownership file 
# 1 row per customer
# 1 column per product 
# 1/0 = owns product Yes/No

pathname <- "C:/Users/mshump/Documents/Projects/product_rec_zeppelin/data/"
setwd(pathname)
df_data_in <- read.csv("prec_study_file_in_cleaned.csv", header = TRUE, stringsAsFactors=FALSE)

#head(df_data_in)
#str(df_data_in)

# focus in on specific products / columns of interest
data_in <- df_data_in[, 7:ncol(df_data_in)]
target_list <- names(data_in)
target_list_num <- length(target_list)

###########
## Begin rule development
############

# for each target product, identify top 3 most related products
# build decision tree from top 3 most related products
# save rule set

tree_collect <- data.frame()
rule_collect <- data.frame()

for(i in 1:target_list_num) { 
  
resp_var <- target_list[i] #iterate through target products
target_id <- match(resp_var,target_list)

message(paste0(resp_var , " has ", length(unique(data_in[ ,target_id])), " levels"))

# if target product has more than 1 level - is owned vs. not owned
  if(length(unique(data_in[ ,target_id])) > 1){

    pred_vars <- target_list[-target_id]
    Formula <- formula(paste(resp_var,"~ ",  paste(pred_vars, collapse=" + ")))
    
    data_in_adj <- data_in
    data_in_adj[,resp_var] <- as.factor(data_in_adj[,resp_var])
    
    ## use RandomForest to identify important variables
    
    rf_fit <- randomForest(Formula, data = data_in_adj , ntree=10, nodesize=30)
    
    varimp <- as.data.frame((rf_fit$importance))
    varimp$var_names <- rownames(varimp)
    varimp <- varimp[order(-varimp$MeanDecreaseGini),]
    
    ###### select top N=3 variables - subjective
    
    top_n_var <- 3
    selected_pred_vars <- varimp [1:top_n_var, c("var_names")]
    
    Formula_sel <- formula(paste(resp_var,"~ ",  paste(selected_pred_vars, collapse=" + ")))
    
    # make decision tree with all  combinations of top N=3 variables
    
    
    fit2 <- rpart(Formula_sel, data = data_in_adj, method="class" 
                 ,control = rpart.control(minbucket = 30, cp = -1, maxsurrogate = 10)) 
    
    # save fules into  data frame
    
    tree_tbl_1 <- cbind(leaf_num=rownames(fit2$frame), as.data.frame(fit2$frame[,-c(9)]), as.data.frame(fit2$frame[,9]))
    
    #calc tree metrics
    
    pop_prev <- tree_tbl_1[tree_tbl_1$leaf_num == 1, c("V5")]
    tree_tbl_1$lift_1 <- tree_tbl_1[ , c("V5")] / pop_prev
    
    sql_rule <- parse_tree(df=data_in_adj , model=fit2)
    
    sql_rule_rows <- unlist(strsplit(gsub("case|end", "", sql_rule), "when"))[-1]
    rule_tbl_1 <- cbind(tree_tbl_1[tree_tbl_1$var == "<leaf>",], rule_p1=sql_rule_rows)
    
    # add some context
    
    tree_tbl_2 <- cbind(target_var=rep(resp_var,nrow(tree_tbl_1)),tree_tbl_1)
    rule_tbl_2 <- cbind(target_var=rep(resp_var,nrow(rule_tbl_1)),rule_tbl_1)
    
    
    #stack it up
    message("Add rule for: ", resp_var)
    
    tree_collect <- rbind(tree_collect,tree_tbl_2)
    rule_collect <- rbind(rule_collect,rule_tbl_2)
    
  } else {
    
    message("skip: ", resp_var)
    
  }

}


#head(tree_collect)
#head(rule_collect)

#fwrite(tree_collect, "tree_collect.csv")
#fwrite(rule_collect, "rule_collect.csv")

##########
# Create opportunity table per customer based on rules
##########

oppty_table_collect <- data.frame()

# per rule, create SQL statement with where clause
# applying back to study file customers - but could be applied to rest of active customer base
# using SQLDF to memic the application of these rules to a database table 
#- in practice I use sparkr in a Zeppelin notebook to write to HDFS tables

for(i in 1:nrow(rule_collect)){
  
target_var_tmp <- as.character(rule_collect[i, c("target_var")])
rule_df_tmp <- rule_collect[i,]

rule_where_sql <- paste0(strsplit(as.character(rule_df_tmp[ ,"rule_p1"]), "then")[[1]][1], " and " ,target_var_tmp, " <0.5")

rule_sql_statement <- paste0("select dtoperatingbrand, dtproperty, dtclientid, '", target_var_tmp ,"' as oppty, ", rule_df_tmp[ ,"V5"], " as oppty_prob from df_data_in where ", rule_where_sql)

oppty_table_tmp <- sqldf(rule_sql_statement)

oppty_table_collect <- rbind(oppty_table_collect, oppty_table_tmp)

}

str(oppty_table_collect)

table(oppty_table_collect$oppty)

summary(data_in)











