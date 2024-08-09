<?php

namespace ByteXR\LaravelScoutOpenSearch\Engines;

use ByteXR\LaravelScoutOpenSearch\Factories\SearchFactory;
use ByteXR\LaravelScoutOpenSearch\Services\OpenSearchClient;
use Illuminate\Database\Eloquent\SoftDeletes;
use Illuminate\Support\LazyCollection;
use Laravel\Scout\Builder;
use Laravel\Scout\Jobs\RemoveableScoutCollection;

class OpenSearchEngine extends \Laravel\Scout\Engines\Engine
{
    /**
     * Create a new engine instance.
     *
     * @return void
     */
    public function __construct(
        protected OpenSearchClient $openSearch,
        protected bool $softDelete = false
    ) {
    }

    /**
     * Update the given model in the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     */
    public function update($models): void
    {
        if ($models->isEmpty()) {
            return;
        }

        $index = $models->first()->searchableAs();

        if ($this->usesSoftDelete($models->first()) && $this->softDelete) {
            $models->each->pushSoftDeleteMetadata();
        }

        $objects = $models->map(function ($model) {
            if (empty($searchableData = $model->toSearchableArray())) {
                return;
            }

            return array_merge(
                $searchableData,
                $model->scoutMetadata(),
                ['objectID' => $model->getScoutKey()],
            );
        })->filter()->values();

        if (! empty($objects)) {
            $this->openSearch->bulkUpdate($index, $objects);
        }
    }

    /**
     * Remove the given model from the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     * @return void
     */
    public function delete($models)
    {
        if ($models->isEmpty()) {
            return;
        }

        $index = $models->first()->searchableAs();

        $keys = $models instanceof RemoveableScoutCollection
            ? $models->pluck($models->first()->getScoutKeyName())
            : $models->map->getScoutKey();

        $this->openSearch->bulkDelete($index, $keys);
    }

    /**
     * Perform the given search on the engine.
     *
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'size' => $builder->limit,
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $index = $builder->index ?: $builder->model->searchableAs();
        $searchBody = SearchFactory::create($builder, $options);

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->openSearch,
                $searchBody,
            );
        }

        return $this->openSearch->search(
            $index,
            $searchBody->toArray(),
        );
    }

    /**
     * Determine if the given model uses soft deletes.
     */
    protected function usesSoftDelete(\Illuminate\Database\Eloquent\Model $model): bool
    {
        return in_array(SoftDeletes::class, class_uses_recursive($model));
    }

    public function paginate(Builder $builder, $perPage, $page)
    {
        return $this->performSearch($builder, [
            'size' => $perPage,
            'from' => ($page - 1) * $perPage,
        ]);
    }

    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    public function map(Builder $builder, $results, $model)
    {
        $results = $results['hits']['hits'];

        if (count($results) === 0) {
            return $model->newCollection();
        }

        $objectIds = collect($results)->pluck('_id')->values()->all();

        $objectIdPositions = array_flip($objectIds);

        $models = $model->getScoutModelsByIds(
            $builder, $objectIds
        )->filter(function ($model) use ($objectIds) {
            return in_array($model->getScoutKey(), $objectIds);
        })->sortBy(function ($model) use ($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        });

        return $models->map(function ($model) use ($results) {
            if ($model->appendsRawSearchResults ?? false) {
                $result = collect($results)->firstWhere('_id', $model->id);

                if ($result) {
                    $model->rawSearchResult = $result['_source'] ?? [];
                }
            }

            return $model;
        });
    }

    public function lazyMap(Builder $builder, $results, $model)
    {
        $results = $results['hits']['hits'];

        if (count($results) === 0) {
            return LazyCollection::make($model->newCollection());
        }

        $objectIds = collect($results)->pluck('_id')->values()->all();
        $objectIdPositions = array_flip($objectIds);

        return $model->queryScoutModelsByIds(
            $builder, $objectIds
        )->cursor()->filter(function ($model) use ($objectIds) {
            return in_array($model->getScoutKey(), $objectIds);
        })->sortBy(function ($model) use ($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        })->values();
    }

    public function getTotalCount($results)
    {
        return $results['hits']['total']['value'];
    }

    public function flush($model)
    {
        $this->openSearch->deleteIndex($model->searchableAs());
    }

    public function createIndex($name, array $options = [])
    {
        $this->openSearch->createIndex($name, $options);
    }

    public function updateIndexSettings($name, array $options = [])
    {
        $this->openSearch->updateIndex($name, $options);
    }

    public function deleteIndex($name)
    {
        $this->openSearch->deleteIndex($name);
    }

    public function deleteAllIndexes()
    {
        $this->openSearch->deleteAllIndexes();
    }
}
