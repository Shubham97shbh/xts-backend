from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from django.contrib.auth.password_validation import validate_password
from rest_framework.validators import UniqueValidator
from rest_framework.serializers import (
    ModelSerializer,
    CharField,
    EmailField,
    ValidationError,
)
from .models import CustomUser
from django.utils import timezone


class MyTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)

        token["username"] = user.username
        token["email"] = user.email

        return token


class RegisterSerializer(ModelSerializer):
    password = CharField(write_only=True, required=True, validators=[validate_password])
    password2 = CharField(
        write_only=True, required=True, validators=[validate_password]
    )
    email = EmailField(
        required=True,
        validators=[
            UniqueValidator(
                queryset=CustomUser.objects.all(),
            )
        ],
    )

    class Meta:
        model = CustomUser
        fields = (
            "username",
            "email",
            "password",
            "password2",
            "role",
            "last_seen",
            "created_at",
        )

    def validate(self, attrs):
        if attrs["password"] != attrs["password2"]:
            raise ValidationError({"password": "Password fields didn't match."})

        return attrs

    def create(self, validated_data):
        user = CustomUser.objects.create(
            username=validated_data["username"],
            email=validated_data["email"],
            role=validated_data.get("role", "user"),
            last_seen=timezone.now(),
        )

        user.set_password(validated_data["password"])
        user.save()

        return user
