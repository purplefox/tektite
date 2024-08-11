## Overview

Tektite website is built using [Hugo](https://www.gohugo.io) , a static site generator written in Go and the theme is based on the [Up Business](https://themes.gohugo.io/themes/up-business-theme/) theme.

## Build from source

1. Install [Hugo](https://gohugo.io/categories/installation/)
2. Clone this repository
3. Switch to the website branch
4. Run `hugo server` to start the development server
5. Open your browser and navigate to `http://localhost:1313`

## Directory Structure

The following is a brief overview of the directory structure of the Tektite website:

```plaintext
content/            # Markdown content files like about page, blog posts, etc.
data/               # Data files with the contet of the different sections of the the home page like capabilities, use cases, etc.
docs/               # Tektite documentation
public/             # Generated site
resources/          # Generated resources like images, etc.
themes/
    tektite-theme/
        assets/     # CSS, JS, images, etc.
        layouts/    # HTML templates which define how the content is rendered
...
```
