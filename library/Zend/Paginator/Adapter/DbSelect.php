<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Paginator
 */

namespace Zend\Paginator\Adapter;

use Zend\Db\Sql;
use Zend\Db\Adapter;
use Zend\Db\Adapter\Driver\ResultInterface;
use Zend\Db\ResultSet\ResultSet;

/**
 * @category   Zend
 * @package    Zend_Paginator
 */
class DbSelect implements AdapterInterface
{
    /**
     * Name of the row count column
     *
     * @var string
     */
    const ROW_COUNT_COLUMN = 'zend_paginator_row_count';

    /**
     * The COUNT query
     *
     * @var \Zend\Db\Sql\Select
     */
    protected $_countSelect = null;

    /**
     * Database query
     *
     * @var \Zend\Db\Sql\Select
     */
    protected $_select = null;

    /**
     * Database sql
     *
     * @var \Zend\Db\Sql\Sql
     */

    protected $_sql = null;

    /**
     * Database adapter
     *
     * @var \Zend\Db\Adapter\Adapter
     */
    protected $_adapter = null;


    /**
     * Total item count
     *
     * @var integer
     */
    protected $_rowCount = null;

    /**
     * Constructor.
     *
     * @param \Zend\Db\Sql\Select $select The select query
     * @param \Zend\Db\Adapter $adapter the current adapter
     */
    public function __construct(Sql\Select $select, Adapter\Adapter $adapter)
    {
        $this->_select = $select;
        $this->_adapter = $adapter;
        $this->_sql = new Sql\Sql($adapter);
    }

    /**
     * Sets the total row count, either directly or through a supplied
     * query.  Without setting this, {@link getPages()} selects the count
     * as a subquery (SELECT COUNT ... FROM (SELECT ...)).  While this
     * yields an accurate count even with queries containing clauses like
     * LIMIT, it can be slow in some circumstances.  For example, in MySQL,
     * subqueries are generally slow when using the InnoDB storage engine.
     * Users are therefore encouraged to profile their queries to find
     * the solution that best meets their needs.
     *
     * @param  \Zend\Db\Sql\Select|integer $rowCount Total row count integer
     *                                               or query
     * @throws Exception\InvalidArgumentException
     * @return DbSelect
     */
    public function setRowCount($rowCount)
    {
        if ($rowCount instanceof Sql\Select) {
            $selectString = $this->_sql->getSqlStringForSqlObject($rowCount);
            $statement = $this->_adapter->query($selectString);

            $rowCountColumn = self::ROW_COUNT_COLUMN;

            // The select query can contain only one column, which should be the row count column
            if (false === strpos($selectString, $rowCountColumn)) {
                throw new Exception\InvalidArgumentException('Row count column not found');
            }

            $result = $statement->execute()->current();

            $this->_rowCount = count($result) > 0 ? $result[$rowCountColumn] : 0;
        } else if (is_integer($rowCount)) {
            $this->_rowCount = $rowCount;
        } else {
            throw new Exception\InvalidArgumentException('Invalid row count');
        }
        return $this;
    }

    /**
     * Returns an array of items for a page.
     *
     * @param  integer $offset           Page offset
     * @param  integer $itemCountPerPage Number of items per page
     * @throws Exception\InvalidArgumentException
     * @return Resultset $resultSet
     */
    public function getItems($offset, $itemCountPerPage)
    {
        $this->_select->offset($offset);
        $this->_select->limit($itemCountPerPage);

        $selectString = $this->_sql->getSqlStringForSqlObject($this->_select);

        $statement = $this->_adapter->query($selectString);
        $result = $statement->execute();

        if ($result instanceof ResultInterface && $result->isQueryResult()) {
            $resultSet = new ResultSet;
            $resultSet->initialize($result);

            return $resultSet;
        } else {
            throw new Exception\InvalidArgumentException('Invalid result');
        }
    }

    /**
     * Returns the total number of rows in the result set.
     *
     * @return integer
     */
    public function count()
    {
        if ($this->_rowCount === null) {
            $this->setRowCount(
                $this->getCountSelect()
            );
        }

        return $this->_rowCount;
    }

    /**
     * Get the COUNT select object for the provided query
     *
     * TODO: Have a look at queries that have both GROUP BY and DISTINCT specified.
     * In that use-case I'm expecting problems when either GROUP BY or DISTINCT
     * has one column.
     *
     * @return \Zend\Db\Sql\Select
     */
    public function getCountSelect()
    {
        /**
         * We only need to generate a COUNT query once. It will not change for
         * this instance.
         */
        if ($this->_countSelect !== null) {
            return $this->_countSelect;
        }

        $rowCount = clone $this->_select;
        $platform = $this->_adapter->platform;

        $countColumn = $platform->quoteIdentifier(self::ROW_COUNT_COLUMN);
        $countPart   = 'COUNT(1) AS ';
        $groupPart   = null;

        $columnPart = $rowCount->getRawState('columns');
        $tablePart = $rowCount->getRawState('table');
        $groupParts  = $rowCount->getRawState('group');
        $havingParts = $rowCount->getRawState('having');

        /**
         * If there is more than one column AND it's a DISTINCT query, more
         * than one group, or if the query has a HAVING clause, then take
         * the original query and use it as a subquery os the COUNT query.
         */
        if (count($groupParts) > 1 || ($havingParts->count() > 0)) {
            $sqlString = $this->_select->getSqlString();

            $tablePart = array('t' => $sqlString);

        } else if (!empty($groupParts) && $groupParts[0] !== Sql\Select::SQL_STAR &&
            !($groupParts[0] instanceof Sql\ExpressionInterface)
        ) {
            $groupPart = $platform->quoteIdentifierInFragment($groupParts[0]);
        }

        /**
         * If the original query had a GROUP BY or a DISTINCT part and only
         * one column was specified, create a COUNT(DISTINCT ) query instead
         * of a regular COUNT query.
         */
        if (!empty($groupPart)) {
            $countPart = 'COUNT(DISTINCT ' . $groupPart . ') AS ';
        }

        /**
         * Create the COUNT part of the query
         */
        $expression = new Sql\Expression($countPart . $countColumn);

        $select = new Sql\Select();
        $select->from($tablePart)
            ->columns(array($expression));

        $this->_countSelect = $select;

        return $select;
    }
}
