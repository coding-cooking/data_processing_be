# Generated by Django 5.1.2 on 2024-10-20 10:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_processor', '0002_processedfile_delete_uploadedfile'),
    ]

    operations = [
        migrations.AlterField(
            model_name='processedfile',
            name='file',
            field=models.FileField(upload_to='process/'),
        ),
    ]
