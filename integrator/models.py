from django.db import models

class ProductSync(models.Model):
    sku = models.CharField(max_length=64, unique=True)
    data_dict = models.CharField(max_length=255)
    data_hash = models.CharField(max_length=64)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.sku
    
class DataQualityLog(models.Model):
    sku = models.CharField(max_length=64, unique=True)
    data_dict = models.CharField(max_length=1024)
    error_message = models.CharField(max_length=1024)
    data_hash = models.CharField(max_length=64)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.sku