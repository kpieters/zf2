<?php

/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Session
 */

namespace Zend\Session\SaveHandler;

use Mongo;
use MongoDate;
use Zend\Session\Exception\InvalidArgumentException;

/**
 * MongoDB session save handler
 *
 * @category   Zend
 * @package    Zend_Session
 * @subpackage SaveHandler
 */
class MongoDB implements SaveHandlerInterface
{
    /**
     * MongoCollection instance
     *
     * @var MongoCollection
     */
    protected $mongoCollection;

    /**
     * Session name
     *
     * @var string
     */
    protected $sessionName;

    /**
     * Session lifetime
     *
     * @var int
     */
    protected $lifetime;

    /**
     * MongoDB session save handler options
     * @var MongoDBOptions
     */
    protected $options;

    /**
     * Constructor
     *
     * @param Mongo $mongo
     * @param MongoDBOptions $options
     * @throws Zend\Session\Exception\InvalidArgumentException
     */
    public function __construct(Mongo $mongo, MongoDBOptions $options)
    {
        if (null === ($database = $options->getDatabase())) {
            throw new InvalidArgumentException('The database option cannot be emtpy');
        }

        if (null === ($collection = $options->getCollection())) {
            throw new InvalidArgumentException('The collection option cannot be emtpy');
        }

        $this->mongoCollection = $mongo->selectCollection($database, $collection);
        $this->options = $options;
    }

    /**
     * Open session
     *
     * @param string $savePath
     * @param string $name
     * @return boolean
     */
    public function open($savePath, $name)
    {
        // Note: session save path is not used
        $this->sessionName = $name;
        $this->lifetime    = ini_get('session.gc_maxlifetime');

        return true;
    }

    /**
     * Close session
     *
     * @return boolean
     */
    public function close()
    {
        return true;
    }

    /**
     * Read session data
     *
     * @param string $id
     * @return string
     */
    public function read($id)
    {
        $session = $this->mongoCollection->findOne(array(
            '_id' => $id,
            $this->options->getNameField() => $this->sessionName,
        ));

        if (null !== $session) {
            if ($session[$this->options->getModifiedField()] instanceof MongoDate &&
                $session[$this->options->getModifiedField()]->sec +
                $session[$this->options->getLifetimeField()] > time()) {
                return $session[$this->options->getDataField()];
            }
            $this->destroy($id);
        }

        return '';
    }

    /**
     * Write session data
     *
     * @param string $id
     * @param string $data
     * @return boolean
     */
    public function write($id, $data)
    {
        $saveOptions = array_replace(
            $this->options->getSaveOptions(),
            array('upsert' => true, 'multiple' => false)
        );

        $criteria = array(
            '_id' => $id,
            $this->options->getNameField() => $this->sessionName,
        );

        $newObj = array('$set' => array(
            $this->options->getDataField() => (string) $data,
            $this->options->getLifetimeField() => $this->lifetime,
            $this->options->getModifiedField() => new MongoDate(),
        ));

        /* Note: a MongoCursorException will be thrown if a record with this ID
         * already exists with a different session name, since the upsert query
         * cannot insert a new document with the same ID and new session name.
         * This should only happen if ID's are not unique or if the session name
         * is altered mid-process.
         */
        $result = $this->mongoCollection->update($criteria, $newObj, $saveOptions);

        return (bool) (isset($result['ok']) ? $result['ok'] : $result);
    }

    /**
     * Destroy session
     *
     * @param string $id
     * @return boolean
     */
    public function destroy($id)
    {
        $result = $this->mongoCollection->remove(array(
            '_id' => $id,
            $this->options->getNameField() => $this->sessionName,
        ), $this->options->getSaveOptions());

        return (bool) (isset($result['ok']) ? $result['ok'] : $result);
    }

    /**
     * Garbage collection
     *
     * Note: MongoDB 2.2+ supports TTL collections, which may be used in place
     * of this method by indexing with "modified" field with an
     * "expireAfterSeconds" option.
     *
     * @see http://docs.mongodb.org/manual/tutorial/expire-data/
     * @param int $maxlifetime
     * @return boolean
     */
    public function gc($maxlifetime)
    {
        /* Note: unlike DbTableGateway, we do not use the lifetime field in
         * each document. Doing so would require a $where query to work with the
         * computed value (modified + lifetime) and be very inefficient.
         */
        $result = $this->mongoCollection->remove(array(
            $this->options->getModifiedField() => array('$lt' => new MongoDate(time() - $maxlifetime)),
        ), $this->options->getSaveOptions());

        return (bool) (isset($result['ok']) ? $result['ok'] : $result);
    }
}
