# Generated by Django 5.0.4 on 2024-05-09 18:33

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='SharedObject',
            fields=[
                ('key', models.IntegerField(primary_key=True, serialize=False)),
                ('value', models.TextField()),
            ],
        ),
    ]