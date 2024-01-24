terraform {    
  required_providers {    
    azurerm = {    
      source = "hashicorp/azurerm"    
    }    
  }    
} 
   
provider "azurerm" {    
  features {}  
  tenant_id = "571c8018-2e13-4657-af22-5b95a5e78fcd"  
}


# Generate a random integer to create a globally unique name
resource "random_integer" "ri" {
  min = 10000
  max = 99999
}