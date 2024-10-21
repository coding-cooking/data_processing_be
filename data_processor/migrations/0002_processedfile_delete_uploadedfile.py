# Generated by Django 5.1.2 on 2024-10-20 04:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_processor', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProcessedFile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file', models.FileField(upload_to='uploads/')),
                ('processed_data', models.JSONField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.DeleteModel(
            name='UploadedFile',
        ),
    ]
