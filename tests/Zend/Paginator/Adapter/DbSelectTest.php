<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Paginator
 */

namespace ZendTest\Paginator\Adapter;

use Zend\Paginator\Adapter;
use Zend\Db\Adapter as DbAdapter;
use Zend\Db\Sql;
use Zend\Paginator\Exception;

require_once __DIR__ . '/../_files/TestTable.php';

/**
 * @category   Zend
 * @package    Zend_Paginator
 * @subpackage UnitTests
 * @group      Zend_Paginator
 */
class DbSelectTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Zend\Db\Adapter\DbSelect
     */
    protected $adapter;

    /**
     * @var Zend\Db\Adapter\Adapter
     */
    protected $db;

    /**
     * @var \Zend\Db\Sql\Sql
     */
    protected $sql;

    /**
     * @var \Zend\Db\Sql\Select
     */
    protected $query;

    /**
     * @var \Zend\Db\TableGateway\TableGateway
     */
    protected $table;

    /**
     * Prepares the environment before running a test.
     */
    protected function setUp()
    {
        if (!extension_loaded('pdo_sqlite')) {
            $this->markTestSkipped('Pdo_Sqlite extension is not loaded');
        }

        parent::setUp();

        $this->db = new DbAdapter\Adapter(array(
                                                'driver'   => 'Pdo_Sqlite',
                                                'database' =>  __DIR__ . '/../_files/test.sqlite',
                                           ));

        $this->table = new \ZendTest\Paginator\TestAsset\TestTable('test', $this->db);

        $this->query = new Sql\Select;
        $this->query->from('test')
            ->order('number ASC'); // ZF-3740
        //->limit(1000, 0); // ZF-3727

        $this->sql = new Sql\Sql($this->db);

        $this->adapter = new Adapter\DbSelect(array(
                                                    'select_query' => $this->query,
                                                    'db_adapter'   => $this->db,
                                               ), 'dbquery');
    }
    /**
     * Cleans up the environment after running a test.
     */
    protected function tearDown()
    {
        $this->adapter = null;
        parent::tearDown();
    }

    public function testGetsItemsAtOffsetZero()
    {
        $actual = $this->adapter->getItems(0, 10);

        $i = 1;
        foreach ($actual as $item) {
            $this->assertEquals($i, $item['number']);
            $i++;
        }
    }

    public function testGetsItemsAtOffsetTen()
    {
        $actual = $this->adapter->getItems(10, 10);

        $i = 11;
        foreach ($actual as $item) {
            $this->assertEquals($i, $item['number']);
            $i++;
        }
    }

    public function testAcceptsIntegerValueForRowCount()
    {
        $this->adapter->setRowCount(101);
        $this->assertEquals(101, $this->adapter->count());
    }

    public function testThrowsExceptionIfInvalidQuerySuppliedForRowCount()
    {
        $this->setExpectedException('Zend\Paginator\Adapter\Exception\InvalidArgumentException', 'Row count column not found');
        $select = $this->sql->select();
        $select->from('test');
        $this->adapter->setRowCount($select);
    }

    public function testThrowsExceptionIfInvalidQuerySuppliedForRowCount2()
    {
        $wrongcolumn = $this->db->getPlatform()->quoteIdentifier('wrongcolumn');
        $expr = new Sql\Expression("COUNT(*) AS $wrongcolumn");
        $query = new Sql\Select;
        $query->from('test')->columns(array($expr));

        $this->setExpectedException('Zend\Paginator\Adapter\Exception\InvalidArgumentException', 'Row count column not found');
        $this->adapter->setRowCount($query);
    }

    public function testAcceptsQueryForRowCount()
    {
        $row_count_column = $this->db->getPlatform()->quoteIdentifier(Adapter\DbSelect::ROW_COUNT_COLUMN);
        $expression = new Sql\Expression("COUNT(*) AS $row_count_column");

        $rowCount = clone $this->query;
        $rowCount->columns(array($expression));

        $this->adapter->setRowCount($rowCount);

        $this->assertEquals(500, $this->adapter->count());
    }

    public function testThrowsExceptionIfInvalidRowCountValueSupplied()
    {
        $this->setExpectedException('Zend\Paginator\Adapter\Exception\InvalidArgumentException', 'Invalid row count');
        $this->adapter->setRowCount('invalid');
    }

    public function testReturnsCorrectCountWithAutogeneratedQuery()
    {
        $expected = 500;
        $actual = $this->adapter->count();

        $this->assertEquals($expected, $actual);
    }

    public function testDbTableSelectDoesNotThrowException()
    {
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $this->table->getSql()->select(),
                                        ), 'dbselect');
        $count = $adapter->count();
        $this->assertEquals(500, $count);
    }

    /**
     * @group ZF-4001
     */
    public function testGroupByQueryReturnsOneRow()
    {
        $query = new Sql\Select;
        $query->from('test')
            ->order('number ASC')
            ->limit(1000, 0)
            ->group('number');

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');

        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-4001
     */
    public function testGroupByQueryOnEmptyTableReturnsRowCountZero()
    {
        $db = new DbAdapter\Adapter(array(
                                         'driver'   => 'Pdo_Sqlite',
                                         'database' =>  __DIR__ . '/../_files/testempty.sqlite',
                                    ));

        $query = new Sql\Select;
        $query->from('test')
            ->order('number ASC')
            ->limit(1000, 0);
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $db,
                                             'select_query' => $query,
                                        ), 'dbselect');

        $this->assertEquals(0, $adapter->count());
    }

    /**
     * @group ZF-4001
     */
    public function testGroupByQueryReturnsCorrectResult()
    {
        $query = new Sql\Select;
        $query->from('test')
            ->order('number ASC')
            ->limit(1000, 0)
            ->group('testgroup');
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');


        $this->assertEquals(2, $adapter->count());
    }

    /**
     * @group ZF-4032
     * @group ZF2-424
     */
    public function testDistinctColumnQueryReturnsCorrectResult()
    {
        $expr = new Sql\Expression("DISTINCT testgroup");
        $query = new Sql\Select;
        $query->from('test')
            ->columns(array($expr))
            ->order('number ASC')
            ->limit(1000, 0);
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');

        $this->assertEquals(2, $adapter->count());
    }

    /**
     * @group ZF-4094
     */
    public function testSelectSpecificColumns()
    {
        $number = $this->db->getPlatform()->quoteIdentifier('number');
        $query = $this->sql->select()->from('test', array('testgroup', 'number'))
            ->where("$number >= ?", '1');
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');


        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-4177
     */
    public function testSelectDistinctAllUsesRegularCountAll()
    {
        $query = $this->sql->select()->from('test');
        //->distinct();
        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');


        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-5233
     */
    public function testSelectHasAliasedColumns()
    {
        $db = $this->db;

        $db->query('DROP TABLE IF EXISTS `sandboxTransaction`', DbAdapter\Adapter::QUERY_MODE_EXECUTE);
        $db->query('DROP TABLE IF EXISTS `sandboxForeign`', DbAdapter\Adapter::QUERY_MODE_EXECUTE);

        // A transaction table
        $db->query(
            'CREATE TABLE `sandboxTransaction` (
                `id` INTEGER PRIMARY KEY,
                `foreign_id` INT( 1 ) NOT NULL ,
                `name` TEXT NOT NULL
            ) ',
            DbAdapter\Adapter::QUERY_MODE_EXECUTE);

        // A foreign table
        $db->query(
            'CREATE TABLE `sandboxForeign` (
                `id` INTEGER PRIMARY KEY,
                `name` TEXT NOT NULL
            ) ',
            DbAdapter\Adapter::QUERY_MODE_EXECUTE);

        // Insert some data
        $db->query("INSERT INTO `sandboxTransaction` (`foreign_id`,`name`) VALUES ('1','transaction 1 with foreign_id 1');", DbAdapter\Adapter::QUERY_MODE_EXECUTE);
        $db->query("INSERT INTO `sandboxTransaction` (`foreign_id`,`name`) VALUES ('1','transaction 2 with foreign_id 1');", DbAdapter\Adapter::QUERY_MODE_EXECUTE);
        $db->query("INSERT INTO `sandboxForeign` (`name`) VALUES ('John Doe');", DbAdapter\Adapter::QUERY_MODE_EXECUTE);
        $db->query("INSERT INTO `sandboxForeign` (`name`) VALUES ('Jane Smith');", DbAdapter\Adapter::QUERY_MODE_EXECUTE);

        //@todo check which column to use for distinct, not sure if it's the right one
        $expr = new Sql\Expression("DISTINCT foreign_id");
        $query = new Sql\Select();
        $query->columns(array($expr))
            ->from(array('a'=>'sandboxTransaction'), array())
            ->join(array('b'=>'sandboxForeign'), 'a.foreign_id = b.id', array('name'));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $query,
                                        ), 'dbselect');

        $this->assertEquals(1, $adapter->count());
    }

    /**
     * @group ZF-5956
     */
    public function testUnionSelect()
    {
        $this->markTestSkipped('Union not fully implemented (ZF2-424)');

        $union = $this->db->select()->union(array(
                                                 $this->db->select()->from('test')->where('number <= 250'),
                                                 $this->db->select()->from('test')->where('number > 250')
                                            ));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $union,
                                        ), 'dbselect');

        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-7045
     */
    public function testGetCountSelect()
    {
        $this->markTestSkipped('Union not fully implemented (ZF2-424)');

        $union = $this->db->select()->union(array(
                                                 $this->db->select()->from('test')->where('number <= 250'),
                                                 $this->db->select()->from('test')->where('number > 250')
                                            ));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $union,
                                        ), 'dbselect');

        $expected = 'SELECT COUNT(1) AS "zend_paginator_row_count" FROM (SELECT "test".* FROM "test" WHERE (number <= 250) UNION SELECT "test".* FROM "test" WHERE (number > 250)) AS "t"';
        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
    }


    /**
     * @group ZF-5295
     * @group ZF2-424
     */
    public function testMultipleDistinctColumns()
    {
        $this->markTestSkipped('Distinct not fully implemented (ZF2-424)');
        $this->markTestSkipped('Subquery not clear');

        $expr = new Sql\Expression("DISTINCT testgroup");
        $select = new Sql\Select;
        $select->from('test')
            ->columns(array($expr, 'number'));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');

        $expected = 'SELECT COUNT(1) AS "zend_paginator_row_count" FROM (SELECT DISTINCT "test"."testgroup", "test"."number" FROM "test") AS "t"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-5295
     * @group ZF2-424
     */
    public function testSingleDistinctColumn()
    {
//        $this->markTestSkipped('Distinct not fully implemented (ZF2-424)');
        $expr = new Sql\Expression("DISTINCT testgroup");
        $expr = new Sql\Expression(
            'DISTINCT ?',
            'testgroup',
            array(Sql\Expression::TYPE_IDENTIFIER));
        $select = $this->sql->select()->from('test')
            ->columns(array($expr));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');


        $expected = 'SELECT COUNT(DISTINCT "test"."testgroup") AS "zend_paginator_row_count" FROM "test"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(2, $adapter->count());
    }

    /**
     * @group ZF-6330
     */
    public function testGroupByMultipleColumns()
    {
        $select = $this->sql->select()->from('test')
            ->columns(array('testgroup'))
            ->group(array('number', 'testgroup'));

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');

        $expected = 'SELECT COUNT(1) AS "zend_paginator_row_count" FROM (SELECT "test"."testgroup" FROM "test" GROUP BY "number"' . ",\n\t" . '"testgroup") AS "t"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(500, $adapter->count());
    }

    /**
     * @group ZF-6330
     */
    public function testGroupBySingleColumn()
    {
        $select = $this->sql->select()->from('test')
            ->columns(array('testgroup'))
            ->group('test.testgroup');

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');


        $expected = 'SELECT COUNT(DISTINCT "test"."testgroup") AS "zend_paginator_row_count" FROM "test"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(2, $adapter->count());
    }

    /**
     * @group ZF-6562
     */
    public function testSelectWithHaving()
    {
        $select = $this->sql->select()->from('test')
            ->group('number')
            ->having('number > 250');

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');


        $expected = 'SELECT COUNT(1) AS "zend_paginator_row_count" FROM (SELECT "test".* FROM "test" GROUP BY "number" HAVING (number > 250)) AS "t"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(250, $adapter->count());
    }

    /**
     * @group ZF-7127
     */
    public function testMultipleGroupSelect()
    {
        $select = $this->sql->select()->from('test')
            ->group('testgroup')
            ->group('number')
            ->where('number > 250');

        $adapter = new Adapter\DbSelect(array(
                                             'db_adapter'   => $this->db,
                                             'select_query' => $select,
                                        ), 'dbselect');


        $expected = 'SELECT COUNT(1) AS "zend_paginator_row_count" FROM (SELECT "test".* FROM "test" WHERE (number > 250) GROUP BY "testgroup"' . ",\n\t" . '"number") AS "t"';

        $this->assertEquals($expected, $adapter->getCountSelect()->getSqlString());
        $this->assertEquals(250, $adapter->count());
    }

    /**
     * @group ZF-10704
     */
    public function testObjectSelectWithBind()
    {
        $this->markTestSkipped('Test in need of updating for ZF2 (See ZF2-424)');

        $select = $this->db->select();
        $select->from('test')
            ->columns(array('number'))
            ->where('number = ?')
            ->distinct(true)
            ->bind(array(250));

        $adapter = new Adapter\DbSelect($select);
        $this->assertEquals(1, $adapter->count());

        $select->reset(Sql\Select::DISTINCT);
        $select2 = clone $select;
        $select2->reset(Sql\Select::WHERE)
            ->where('number = 500');

        $selectUnion = $this->_db
            ->select()
            ->bind(array(250));

        $selectUnion->union(array($select, $select2));
        $adapter = new Adapter\DbSelect($selectUnion);
        $this->assertEquals(2, $adapter->count());
    }

}