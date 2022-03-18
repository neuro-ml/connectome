# Layers

## connectome.Apply

A layer that applies separate functions to each of the specified names.

`Apply` provides a convenient shortcut for transformations that only depend on the previous value of the name.

### Examples

```python
# using
Apply(image=zoom, mask=zoom_binary)


# is the same as using
class Zoom(Transform):
    __inherit__ = True

    def image(image):
        return zoom(image)

    def mask(mask):
        return zoom_binary(mask)
```
