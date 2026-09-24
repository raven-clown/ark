# ARK brand

The mark is an arch: a bridge that is also the letter A, with one point of
data crossing it. ARK is the bridge between Kafka and your app.

| File | Use |
|---|---|
| `ark-wordmark.svg` | The name, with the arch as the A. No background, for dark surfaces |
| `ark-wordmark-light.svg` | The same for light surfaces |
| `ark-logo-mark.svg` | The mark alone, no background, for dark surfaces |
| `ark-logo-mark-light.svg` | The mark alone for light surfaces |
| `ark-logo.svg` | App icon, avatars, anything 48px and up |
| `ark-logo-small.svg` | Favicon and anything 32px or smaller (thicker lines) |
| `ark-lockup.svg` | Mark plus wordmark on the Night background |
| `ark-flow-dark.svg`, `ark-flow-light.svg` | Animated diagram of a message moving through ARK |

On the web and in the README, use the versions without a background and
pick dark or light with `prefers-color-scheme`:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brand/ark-wordmark.svg">
  <img src="brand/ark-wordmark-light.svg" alt="ARK" height="88">
</picture>
```

Colors:

| Name | Hex | Use |
|---|---|---|
| Night | `#0B0D10` | Background |
| Edge | `#1F252D` | Borders, dividers |
| Frost | `#E8EDF2` | The arch, primary text |
| Flow | `#2EE6A6` | The data point; the one accent color, used for "data is moving" |
| Flow Deep | `#0FA97A` | Flow on light backgrounds |
