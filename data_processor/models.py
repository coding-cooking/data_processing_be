from django.db import models

class ProcessedFile(models.Model):
    file = models.FileField(upload_to='process/')
    processed_data = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
