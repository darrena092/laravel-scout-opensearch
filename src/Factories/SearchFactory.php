<?php

namespace ByteXR\LaravelScoutOpenSearch\Factories;

use Laravel\Scout\Builder;
use OpenSearchDSL\BuilderInterface;
use OpenSearchDSL\Query\Compound\BoolQuery;
use OpenSearchDSL\Query\FullText\QueryStringQuery;
use OpenSearchDSL\Query\TermLevel\TermQuery;
use OpenSearchDSL\Query\TermLevel\TermsQuery;
use OpenSearchDSL\Search;
use OpenSearchDSL\Sort\FieldSort;

class SearchFactory
{
    public static function create(Builder $builder, array $options = []): Search
    {
        // A lot of this is from: https://github.com/matchish/laravel-scout-elasticsearch
        $search = new Search();
        $query = new QueryStringQuery($builder->query);

        if ($builder->wheres || $builder->whereIns) {
            $boolQuery = new BoolQuery();

            if ($builder->wheres) {
                foreach ($builder->wheres as $field => $value) {
                    if (!($value instanceof BuilderInterface)) {
                        $value = new TermQuery((string) $field, $value);
                    }
                    $boolQuery->add($value, BoolQuery::FILTER);
                }
            }

            if ($builder->whereIns) {
                foreach ($builder->whereIns as $field => $arrayOfValues) {
                    $boolQuery->add(new TermsQuery((string)$field, $arrayOfValues), BoolQuery::FILTER);
                }
            }

            $search->addQuery($boolQuery);
        } else {
            $search->addQuery($query);
        }

        if (array_key_exists('from', $options)) {
            $search->setFrom($options['from']);
        }

        if (array_key_exists('size', $options)) {
            $search->setSize($options['size']);
        }

        if (!empty($builder->orders)) {
            foreach ($builder->orders as $order) {
                $search->addSort(new FieldSort($order['column'], $order['direction']));
            }
        }

        return $search;
    }
}