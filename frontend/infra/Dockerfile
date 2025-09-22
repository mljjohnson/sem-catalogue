FROM node:lts-alpine3.18

WORKDIR /usr/src/app

ARG FE_ENV

COPY package*.json ./

RUN npm install

COPY . .

RUN if echo "$FE_ENV" | grep -q "staging"; then \
      cp .env.staging .env.production && cp .env.staging .env; \
    fi

EXPOSE 3009

RUN npm run build

CMD ["npm", "start"]