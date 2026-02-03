<?php

namespace Ufo\RpcMercure\Services;

use Closure;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Mercure\Jwt\FactoryTokenProvider;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Ufo\JsonRpcBundle\ConfigService\RpcMainConfig;
use Ufo\JsonRpcBundle\EventDrivenModel\RpcEventFactory;
use Ufo\RpcError\AbstractRpcErrorException;
use Ufo\RpcMercure\DTO\MercureConfig;
use Ufo\RpcMercure\Events\MercureEvent;
use Ufo\RpcMercure\Events\RpcSocketRequestEvent;
use Ufo\RpcMercure\Events\RpcSocketResponseEvent;
use Ufo\RpcMercure\Exceptions\RpcMercureConfigException;
use Ufo\RpcMercure\Exceptions\RpcMercureRequestException;
use Ufo\RpcObject\RpcRequest;

use function explode;
use function json_decode;
use function json_encode;
use function str_replace;
use function str_starts_with;
use function strpos;
use function substr;
use function trim;

use const PHP_EOL;

class RpcSocketTransport
{
    public const string HEART_BIT = ':';
    readonly public MercureConfig $mercureConfig;

    /**
     * @throws RpcMercureConfigException
     */
    public function __construct(
        protected RpcMainConfig $mainConfig,
        protected FactoryTokenProvider $tokenProvider,
        protected HttpClientInterface $http,
        protected RpcEventFactory $eventFactory,
    )
    {
        $this->mercureConfig = MercureConfig::fromRpcAsyncConfig($this->mainConfig->asyncConfig);
    }

    /**
     * @throws AbstractRpcErrorException
     */
    public function request(
        string $method,
        array $params = [],
        ?string $id = null
    ): void
    {
        $this->eventFactory->fire(
            new RpcSocketRequestEvent($method, $params, $id)
        );
    }

    /**
     * @throws AbstractRpcErrorException
     */
    public function fireEvent(
        string $eventName,
        array $eventData = []
    ): void
    {
        $this->request(
            'event.' . $eventName,
            $eventData
        );
    }

    public function response(
        RpcRequest $request,
    ): void
    {
        $this->eventFactory->fire(
            new RpcSocketResponseEvent($request)
        );
    }

    public function fetch(string $topicName, Closure $callback): void
    {
        $url = $this->mercureConfig->dsn . $this->mercureConfig->getTopic($topicName);
        $response = $this->http->request(
            Request::METHOD_GET,
            $url,
            [
                'headers' => [
                    'Authorization' => 'Bearer ' . $this->tokenProvider->getJwt(),
                ],
            ]
        );

        $buffer = '';
        foreach ($this->http->stream($response) as $chunk) {
            if ($chunk->isTimeout()) {
                continue;
            }

            $buffer .= $chunk->getContent();
            while (($pos = strpos($buffer, "\n\n")) !== false) {
                $rawEvent = substr($buffer, 0, $pos);
                $buffer = substr($buffer, $pos + 2);

                $id = null;
                $data = [];

                foreach (explode("\n", $rawEvent) as $line) {
                    if (str_starts_with($line, 'id:')) {
                        $id = trim(substr($line, 3));
                    } elseif (str_starts_with($line, 'data:')) {
                        $data[] = substr($line, 5);
                    }
                }

                if ($id && $data ) {
                    $this->eventFactory->fire(
                        new MercureEvent(
                            $topicName,
                            implode(PHP_EOL, $data),
                            $callback
                        )
                    );
                }
            }
        }

    }
}