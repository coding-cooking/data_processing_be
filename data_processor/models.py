from django.db import models

# Create your models here.
class UploadedFile(models.Model):
    file = models.FileField(upload_to='uploads/')
    processed = models.BooleanField(default=False)
    timestamp = models.DateTimeField(auto_now_add=True)