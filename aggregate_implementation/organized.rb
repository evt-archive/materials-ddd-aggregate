class AccountAggregate
  include Aggregate

  ###########################################################################
  # Attributes
  ###########################################################################
  attribute :id, String
  attribute :customer_id, String
  attribute :balance, Numeric, default: 0
  attribute :opened_time, Time
  attribute :closed_time, Time
  attribute :transaction_position, Integer

  ###########################################################################
  # Command Handlers
  ###########################################################################
  handle Open do |open|
    account_id = open.account_id

    account, version = store.fetch(account_id, include: :version)

    if account.open?
      logger.info(tag: :ignored) { "Command ignored (Command: #{open.message_type}, Account ID: #{account_id}, Customer ID: #{open.customer_id})" }
      return
    end

    time = clock.iso8601

    opened = Opened.follow(open)
    opened.processed_time = time

    stream_name = stream_name(account_id)

    write.(opened, stream_name, expected_version: version)
  end

  handle Close do |close|
    account_id = close.account_id

    account, version = store.fetch(account_id, include: :version)

    if account.closed?
      logger.info(tag: :ignored) { "Command ignored (Command: #{close.message_type}, Account ID: #{account_id})" }
      return
    end

    time = clock.iso8601

    closed = Closed.follow(close)
    closed.processed_time = time

    stream_name = stream_name(account_id)

    write.(closed, stream_name, expected_version: version)
  end

  handle Deposit do |deposit|
    account_id = deposit.account_id

    account, version = store.fetch(account_id, include: :version)

    position = deposit.metadata.global_position

    if account.current?(position)
      logger.info(tag: :ignored) { "Command ignored (Command: #{deposit.message_type}, Account ID: #{account_id}, Account Position: #{account.transaction_position}, Deposit Position: #{position})" }
      return
    end

    time = clock.iso8601

    deposited = Deposited.follow(deposit)
    deposited.processed_time = time
    deposited.transaction_position = position

    stream_name = stream_name(account_id)

    write.(deposited, stream_name, expected_version: version)
  end

  handle Withdraw do |withdraw|
    account_id = withdraw.account_id

    account, version = store.fetch(account_id, include: :version)

    position = withdraw.metadata.global_position

    if account.current?(position)
      logger.info(tag: :ignored) { "Command ignored (Command: #{withdraw.message_type}, Account ID: #{account_id}, Account Position: #{account.transaction_position}, Withdrawal Position: #{position})" }
      return
    end

    time = clock.iso8601

    stream_name = stream_name(account_id)

    unless account.sufficient_funds?(withdraw.amount)
      withdrawal_rejected = WithdrawalRejected.follow(withdraw)
      withdrawal_rejected.time = time
      withdrawal_rejected.transaction_position = position

      write.(withdrawal_rejected, stream_name)

      return
    end

    withdrawn = Withdrawn.follow(withdraw)
    withdrawn.processed_time = time
    withdrawn.transaction_position = position

    write.(withdrawn, stream_name, expected_version: version)
  end

  handle Withdrawn do |withdrawn|
    return unless withdrawn.metadata.reply?

    record_withdrawal = RecordWithdrawal.follow(withdrawn, exclude: [
      :transaction_position,
      :processed_time
    ])

    time = clock.iso8601
    record_withdrawal.processed_time = time

    write.reply(record_withdrawal)
  end

  ###########################################################################
  # Event Projections
  ###########################################################################
  apply Opened do |opened|
    account.id = opened.account_id
    account.customer_id = opened.customer_id

    opened_time = Time.parse(opened.time)

    self.opened_time = opened_time
  end

  apply Deposited do |deposited|
    account.id = deposited.account_id

    amount = deposited.amount

    self.deposit(amount)

    self.transaction_position = deposited.transaction_position
  end

  apply Withdrawn do |withdrawn|
    account.id = withdrawn.account_id

    amount = withdrawn.amount

    self.withdraw(amount)

    self.transaction_position = withdrawn.transaction_position
  end

  apply WithdrawalRejected do |withdrawal_rejected|
    self.transaction_position = withdrawal_rejected.transaction_position
  end

  apply Closed do |closed|
    closed_time = Time.parse(closed.time)

    self.closed_time = closed_time
  end

  ###########################################################################
  # Methods
  ###########################################################################
  def open?
    !opened_time.nil?
  end

  def closed?
    !closed_time.nil?
  end

  def deposit(amount)
    self.balance += amount
  end

  def withdraw(amount)
    self.balance -= amount
  end

  def current?(position)
    return false if transaction_position.nil?

    transaction_position >= position
  end

  def sufficient_funds?(amount)
    balance >= amount
  end
end
