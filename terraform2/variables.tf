
variable "resource_group_name" {
  type        = string
  default     = "ResourceGroup"
  description = "Name of the resource group."
}

 
variable "appserviceplan_name" {
  type        = string
  default     = "ASP_CI_linux"
  description = "name of the app service plan"
}

variable "Webapp_name" {
  type        = string
  default     = "WebApp1"
  description = "name of the webapp"
}

variable "storage_name" {
  type        = string
  default     = "stracc"
  description = "name of the storage account"
}

variable "server_name" {
  type        = string
  default     = "sqlserver"
  description = "name of the sql server name"
}

variable "admin_login" {
  type        = string
  default     = "superadmin"
  description = "name of the server admin"
}

variable "admin_password" {
  type        = string
  default     = "Idea@2023"
  description = "password of the server admin"
}

//variable "sqldb_name" {
  //type        = string
  //default     = "sqlDB"
  //description = "name of the sql DB"
//}