module AccountComponent
  module Handlers
    class Events
      include Log::Dependency
      include Messaging::Handle
      include Messaging::StreamName
      include Messages::Events
      include Messages::Replies

      dependency :write, Messaging::Postgres::Write
      dependency :clock, Clock::UTC

      def configure
        Messaging::Postgres::Write.configure(self)
        Clock::UTC.configure(self)
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
    end
  end
end
