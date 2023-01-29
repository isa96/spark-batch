SELECT 	order_id,
		order_date,
		a.user_id,
		b.payment_name,
		c.shipper_name,
		order_price,
		order_discount,
		d.voucher_name,
		d.voucher_price,
		order_total,
		e.rating_status
FROM tb_orders a 
	LEFT JOIN tb_payments b ON a.payment_id = b.payment_id
	LEFT JOIN tb_shippers c ON a.shipper_id = c.shipper_id 
	LEFT JOIN tb_vouchers d ON a.voucher_id = d.voucher_id
	LEFT JOIN tb_ratings e ON a.rating_id = e.rating_id ;