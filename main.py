import uuid
from typing import Literal

from pydantic import BaseModel, Field

class AutoTypeField:
    def __set_name__(self, owner, name):
        class_name = owner.__name__
        literal_type = Literal[class_name]

        # Inject field dynamically
        owner.__annotations__[name] = literal_type
        setattr(owner, name, Field(default=class_name))


class Image(BaseModel):
    type = AutoTypeField()
    content: str = Field(description='base64 encoded image')


class Sound(BaseModel):
    type = AutoTypeField()
    content: str = Field(description='base64 encoded sound')


class Text(BaseModel):
    type = AutoTypeField()
    content: str = Field(description='plain text')


class Container(BaseModel):
    parts: list[Image | Sound | Text]
    id: uuid.UUID


def main():
    data = dict(
        id=uuid.uuid4(),
        parts=[
            dict(content='hello world', type='Text'),
            dict(content='base64 encoded sound', type='Sound'),
            dict(content='base64 encoded image', type='Image'),
        ],
    )

    container = Container.model_validate(data)
    print(container)


if __name__ == "__main__":
    main()
