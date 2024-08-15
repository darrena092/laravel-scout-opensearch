<?php

namespace ByteXR\LaravelScoutOpenSearch\Services;

use Illuminate\Support\Collection;
use OpenSearch\Client;

class OpenSearchClient
{
    public function __construct(private Client $client)
    {
    }

    public function createIndex(string $index, array $options = []): void
    {
        $data = [
            'index' => $index,
        ];

        if ($options) {
            $data['body'] = $options;
        }

        $this->client->indices()->create($data);
    }

    public function updateIndex(string $index, array $options = []): void
    {
        if (!$this->client->indices()->exists(['index' => $index])) {
            $this->createIndex($index, $options);
        } else {
            $data = [
                'index' => $index,
            ];

            if ($options) {
                $data['body'] = $options;
            }

            $this->client->indices()->putMapping($data);
        }
    }

    public function deleteIndex(string $index): void
    {
        $this->client->indices()->delete([
            'index' => $index,
        ]);
    }

    public function deleteAllIndexes(): void
    {
        $this->client->indices()->delete(['index' => '*']);
    }

    public function bulkUpdate(string $index, $models): callable|array
    {
        $data = [];
        $models->each(function ($model) use ($index, &$data) {
            $data[] = [
                'index' => [
                    '_index' => $index,
                    '_id'    => $model['objectID'],
                ],
            ];
            $data[] = $model;
        });

        return $this->client->bulk([
            'index' => $index,
            'body'  => $data,
        ]);
    }

    public function bulkDelete(string $index, Collection $keys): callable|array
    {
        $data = $keys->map(function ($key) use ($index) {
            return [
                'delete' => [
                    '_index' => $index,
                    '_id'    => $key,
                ],
            ];
        })->toArray();

        return $this->client->bulk([
            'index' => $index,
            'body'  => $data,
        ]);
    }

    public function search(string $index, array $body)
    {
        return $this->client->search([
            'index' => $index,
            'body'  => $body,
        ]);
    }
}
