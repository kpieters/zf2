<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Form
 */

namespace Zend\Form\View\Helper;

use DateTime;
use IntlDateFormatter;
use Locale;
use Zend\Form\ElementInterface;
use Zend\Form\Element\MonthSelect as MonthSelectElement;
use Zend\Form\Exception;

/**
 * @category   Zend
 * @package    Zend_Form
 * @subpackage View
 */
class FormMonthSelect extends AbstractHelper
{
    /**
     * FormSelect helper
     *
     * @var FormSelect
     */
    protected $selectHelper;

    /**
     * Date formatter to use
     *
     * @var int
     */
    protected $dateType = IntlDateFormatter::LONG;

    /**
     * Pattern to use for Date rendering
     *
     * @var string
     */
    protected $pattern;

    /**
     * Locale to use
     *
     * @var string
     */
    protected $locale;


    /**
     * Render a month element that is composed of two selects
     *
     * @param \Zend\Form\ElementInterface $element
     * @throws \Zend\Form\Exception\InvalidArgumentException
     * @throws \Zend\Form\Exception\DomainException
     * @return string
     */
    public function render(ElementInterface $element)
    {
        if (!$element instanceof MonthSelectElement) {
            throw new Exception\InvalidArgumentException(sprintf(
                '%s requires that the element is of type Zend\Form\Element\Select',
                __METHOD__
            ));
        }

        $name = $element->getName();
        if ($name === null || $name === '') {
            throw new Exception\DomainException(sprintf(
                '%s requires that the element has an assigned name; none discovered',
                __METHOD__
            ));
        }

        $selectHelper = $this->getSelectElementHelper();
        $pattern      = $this->parsePattern();

        // The pattern always contains "day" part and the first separator, so we have to remove it
        unset($pattern['day']);
        unset($pattern[0]);

        $monthsOptions = $this->getMonthsOptions($pattern['month']);
        $yearOptions   = $this->getYearsOptions($element->getMinYear(), $element->getMaxYear());

        $monthElement = $element->getMonthElement()->setValueOptions($monthsOptions);
        $yearElement  = $element->getYearElement()->setValueOptions($yearOptions);

        if ($element->shouldCreateEmptyOption()) {
            $monthElement->setEmptyOption('');
            $yearElement->setEmptyOption('');
        }

        $markup = array();
        $markup[$pattern['month']] = $selectHelper->render($monthElement);
        $markup[$pattern['year']]  = $selectHelper->render($yearElement);

        $markup = sprintf(
            '%s %s %s',
            $markup[array_shift($pattern)],
            array_shift($pattern), // Delimiter
            $markup[array_shift($pattern)]
        );

        return $markup;
    }

    /**
     * Invoke helper as function
     *
     * Proxies to {@link render()}.
     *
     * @param \Zend\Form\ElementInterface $element
     * @param int                         $dateType
     * @param null|string                 $locale
     * @return FormDateSelect
     */
    public function __invoke(ElementInterface $element = null, $dateType = IntlDateFormatter::LONG, $locale = null)
    {
        if (!$element) {
            return $this;
        }

        $this->setDateType($dateType);

        if ($locale !== null) {
            $this->setLocale($locale);
        }

        return $this->render($element);
    }

    /**
     * @return string
     */
    public function getPattern()
    {
        if ($this->pattern === null) {
            $intl           = new IntlDateFormatter($this->getLocale(), $this->dateType, IntlDateFormatter::NONE);
            $this->pattern  = $intl->getPattern();
        }

        return $this->pattern;
    }

    /**
     * Parse the pattern
     *
     * @return array
     */
    protected function parsePattern()
    {
        $pattern    = $this->getPattern();
        $pregResult = preg_split('/([ -,.\/]+)/', $pattern, -1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);

        $result = array();
        foreach ($pregResult as $value) {
            if (stripos($value, 'd') !== false) {
                $result['day'] = $value;
            } elseif (stripos($value, 'm') !== false) {
                $result['month'] = $value;
            } elseif (stripos($value, 'y') !== false) {
                $result['year'] = $value;
            } else {
                $result[] = $value;
            }
        }

        return $result;
    }

    /**
     * @param  int $dateType
     * @return FormDateSelect
     */
    public function setDateType($dateType)
    {
        // The FULL format uses values that are not used
        if ($dateType === IntlDateFormatter::FULL) {
            $dateType = IntlDateFormatter::LONG;
        }

        $this->dateType = $dateType;

        return $this;
    }

    /**
     * @return int
     */
    public function getDateType()
    {
        return $this->dateType;
    }

    /**
     * @param  string $locale
     * @return FormDateSelect
     */
    public function setLocale($locale)
    {
        $this->locale = $locale;
        return $this;
    }

    /**
     * @return string
     */
    public function getLocale()
    {
        if ($this->locale === null) {
            $this->locale = Locale::getDefault();
        }

        return $this->locale;
    }

    /**
     * Create a key => value options for months
     *
     * @param string $pattern Pattern to use for months
     * @return array
     */
    protected function getMonthsOptions($pattern)
    {
        $keyFormatter   = new IntlDateFormatter($this->getLocale(), null, null, null, null, 'MM');
        $valueFormatter = new IntlDateFormatter($this->getLocale(), null, null, null, null, $pattern);
        $date           = new DateTime('1970-01-01');

        $result = array();
        for ($month = 1; $month <= 12; $month++) {
            $key   = $keyFormatter->format($date);
            $value = $valueFormatter->format($date);
            $result[$key] = $value;

            $date->modify('+1 month');
        }

        return $result;
    }

    /**
     * Create a key => value options for years
     * NOTE: we don't use a pattern for years, as years written as two digits can lead to hard to
     * read date for users, so we only use four digits years
     *
     * @param int $minYear
     * @param int $maxYear
     * @return array
     */
    protected function getYearsOptions($minYear, $maxYear)
    {
        $result = array();
        for ($i = $maxYear; $i >= $minYear; --$i) {
            $result[$i] = $i;
        }

        return $result;
    }

    /**
     * Retrieve the FormSelect helper
     *
     * @return FormRow
     */
    protected function getSelectElementHelper()
    {
        if ($this->selectHelper) {
            return $this->selectHelper;
        }

        if (method_exists($this->view, 'plugin')) {
            $this->selectHelper = $this->view->plugin('formselect');
        }

        return $this->selectHelper;
    }
}

